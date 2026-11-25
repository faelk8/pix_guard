"""Verified capture reader and immutable snapshot publisher."""

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, ExitStack
import heapq

from pixguard_ai.application.export_capture import ExportWorkspace

from pixguard_ai.domain.capture import validate_capture_id, validate_snapshot_id


MAX_JSON_BYTES = 262_144


def utc_now() -> str:
    """Return an explicit UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def canonical(value: object) -> bytes:
    """Encode canonical JSON without NaN or duplicate-field ambiguity."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject ambiguous duplicate JSON keys."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def read_json(path: Path) -> dict[str, Any]:
    """Read one bounded JSON object safely."""
    if path.is_symlink():
        raise ValueError("symlink metadata")
    with path.open("rb") as stream:
        payload = stream.read(MAX_JSON_BYTES + 1)
    if len(payload) > MAX_JSON_BYTES:
        raise ValueError("metadata exceeds limit")
    value = json.loads(payload, object_pairs_hook=unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    if not isinstance(value, dict):
        raise ValueError("metadata must be an object")
    return value


def atomic_json(path: Path, value: object) -> None:
    """Fsync an atomic JSON publication and its directory."""
    payload = canonical(value)
    if len(payload) > MAX_JSON_BYTES:
        raise ValueError("metadata exceeds limit")
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    temporary.unlink(missing_ok=True)


def sync_directory(path: Path) -> None:
    """Fsync a directory's entry changes."""
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def digest(path: Path) -> str:
    """Hash a file in bounded blocks."""
    result = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


class DiskRecords:
    """Replay JSON records from a bounded-memory temporary file."""

    def __init__(self) -> None:
        self.stream = tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b")
        self.count = 0

    def append(self, value: Any) -> None:
        """Append one JSON-compatible record."""
        self.stream.seek(0, 2)
        self.stream.write(json.dumps(value, separators=(",", ":")).encode() + b"\n")
        self.count += 1

    def __iter__(self) -> Iterator[Any]:
        """Replay records without retaining the collection."""
        self.stream.seek(0)
        for line in self.stream:
            yield json.loads(line)

    def __len__(self) -> int:
        """Return the tracked record count without reading the spool."""
        return self.count

    def clear(self) -> None:
        """Reuse storage without retaining a previous event group."""
        self.stream.seek(0)
        self.stream.truncate()
        self.count = 0

    def close(self) -> None:
        """Release scratch storage."""
        self.stream.close()

    def __del__(self) -> None:
        """Release a reader spool if its consumer exits without exhausting it."""
        if hasattr(self, "stream"):
            self.close()


def disk_sorted(records: Iterable[Any], key: Callable[[Any], Any]) -> Iterator[Any]:
    """Sort using 1 MiB runs and at most 32 concurrently open merge inputs."""
    with tempfile.TemporaryDirectory(prefix="pixguard-sort-") as directory:
        root = Path(directory)
        count = 0
        batch: list[Any] = []
        size = 0

        def write_run() -> None:
            nonlocal count, size
            batch.sort(key=key)
            with (root / f"0-{count}").open("w") as stream:
                for item in batch:
                    stream.write(json.dumps(item, separators=(",", ":")) + "\n")
            count += 1
            batch.clear()
            size = 0

        for item in records:
            batch.append(item)
            size += len(json.dumps(item))
            if size >= 1024 * 1024:
                write_run()
        if batch:
            write_run()
        level = 0
        while count > 1:
            output_count = 0
            for start in range(0, count, 32):
                paths = [root / f"{level}-{index}"
                         for index in range(start, min(start + 32, count))]
                with ExitStack() as stack:
                    streams = [stack.enter_context(path.open()) for path in paths]
                    with (root / f"{level + 1}-{output_count}").open("w") as output:
                        for item in heapq.merge(*(map(json.loads, stream) for stream in streams), key=key):
                            output.write(json.dumps(item, separators=(",", ":")) + "\n")
                for path in paths:
                    path.unlink()
                output_count += 1
            level += 1
            count = output_count
        if count:
            with (root / f"{level}-0").open() as stream:
                yield from map(json.loads, stream)


class LocalExportWorkspace:
    """Own disk-backed collections and merge runs for one export."""

    def __init__(self, cleanup: ExitStack) -> None:
        """Register collection lifetimes with the export context."""
        self.cleanup = cleanup

    def collection(self) -> DiskRecords:
        """Allocate replayable storage with at most 1 MiB held in memory."""
        records = DiskRecords()
        self.cleanup.callback(records.close)
        return records

    def ordered(self, records: Iterable[Any], key: Callable[[Any], Any]) -> Iterator[Any]:
        """Return an externally sorted iterator with explicit lifetime cleanup."""
        ordered = disk_sorted(records, key)
        self.cleanup.callback(ordered.close)
        return ordered


class CaptureSnapshot:
    """Read captured records and publish an immutable snapshot namespace."""

    def __init__(self, data_root: Path) -> None:
        """Require an existing non-symlink data root."""
        if data_root.is_symlink():
            raise ValueError("data root is a symlink")
        data_root.mkdir(parents=True, exist_ok=True)
        self.root = data_root.resolve(strict=True)
        self.directory: Path | None = None

    def read_completed(self, capture_id: str) -> tuple[dict[str, Any], Iterable[dict[str, Any]]]:
        """Verify a completed capture manifest and every raw segment record."""
        capture_id = validate_capture_id(capture_id)
        capture_dir = self.root / "captures" / capture_id
        if capture_dir.is_symlink():
            raise ValueError("capture directory is a symlink")
        manifest = read_json(capture_dir / "manifest.json")
        if (manifest.get("capture_manifest_version") != 1
                or manifest.get("status") != "completed"
                or manifest.get("capture_id") != capture_id):
            raise ValueError("capture is not completed")
        segment = capture_dir / "records.jsonl"
        if segment.is_symlink():
            raise ValueError("capture segment is a symlink")
        recorded = manifest["segment"]
        if digest(segment) != recorded["sha256"] or segment.stat().st_size != recorded["size_bytes"]:
            raise ValueError("capture segment checksum mismatch")
        records = DiskRecords()
        start, end = manifest["start"], manifest["end"]

        def verified_records() -> Iterable[dict[str, Any]]:
            with segment.open("rb") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    record = json.loads(line, object_pairs_hook=unique_object)
                    checksum = record.pop("checksum_sha256")
                    if sha256(canonical(record)).hexdigest() != checksum:
                        raise ValueError("capture record checksum mismatch")
                    record["checksum_sha256"] = checksum
                    offset = record["offset"]
                    if type(offset) is not int or offset < start or offset >= end:
                        raise ValueError("capture offset outside frozen range")
                    yield record

        previous = None
        try:
            for record in disk_sorted(verified_records(), key=lambda item: item["offset"]):
                if record["offset"] == previous:
                    raise ValueError("invalid or duplicate capture offset")
                previous = record["offset"]
                records.append(record)
            if manifest["count"] != len(records):
                raise ValueError("capture count mismatch")
        except BaseException:
            records.close()
            raise
        return manifest, records

    @contextmanager
    def workspace(self) -> Iterator[ExportWorkspace]:
        """Release every temporary export resource on success or failure."""
        with ExitStack() as cleanup:
            yield LocalExportWorkspace(cleanup)

    def reserve(self, capture_id: str, snapshot_id: str) -> None:
        """Reserve an exclusive immutable snapshot directory."""
        capture_id = validate_capture_id(capture_id)
        snapshot_id = validate_snapshot_id(snapshot_id)
        capture_dir = self.root / "captures" / capture_id
        snapshots = capture_dir / "snapshots"
        if snapshots.is_symlink():
            raise ValueError("snapshot namespace is a symlink")
        snapshots.mkdir(mode=0o750, exist_ok=True)
        sync_directory(snapshots.parent)
        directory = snapshots / snapshot_id
        if directory.is_symlink():
            raise ValueError("snapshot directory is a symlink")
        directory.mkdir(mode=0o750)
        sync_directory(snapshots)
        self.directory = directory

    def checkpoint(self, manifest: dict[str, Any]) -> None:
        """Persist a snapshot checkpoint."""
        if self.directory is None:
            raise RuntimeError("snapshot is not reserved")
        atomic_json(self.directory / "manifest.json", manifest)

    def publish(self, capture_manifest: dict[str, Any],
                exported: Iterable[bytes], transport_quarantine: Iterable[dict[str, Any]],
                contract_quarantine: Iterable[dict[str, Any]],
                duplicate_count: int) -> dict[str, Any]:
        """Publish snapshot files and reconcile every physical capture coordinate."""
        if self.directory is None:
            raise RuntimeError("snapshot is not reserved")
        events_path = self.directory / "events.jsonl"
        quarantine_path = self.directory / "quarantine.jsonl"
        exported_count = 0
        transport_count = 0
        contract_count = 0
        with events_path.open("xb") as stream:
            for line in exported:
                stream.write(line)
                exported_count += 1
            stream.flush()
            os.fsync(stream.fileno())
        with quarantine_path.open("xb") as stream:
            for items, transport in ((transport_quarantine, True), (contract_quarantine, False)):
                for item in items:
                    stream.write(canonical(item) + b"\n")
                    transport_count += int(transport)
                    contract_count += int(not transport)
            stream.flush()
            os.fsync(stream.fileno())
        sync_directory(self.directory)

        physical = capture_manifest["count"]
        counts = {
            "physical": physical,
            "exported": exported_count,
            "duplicates": duplicate_count,
            "transport_quarantine": transport_count,
            "contract_quarantine": contract_count,
        }
        reconciled = physical == (counts["exported"] + counts["duplicates"]
                                  + counts["transport_quarantine"]
                                  + counts["contract_quarantine"])
        if not reconciled:
            raise ValueError("snapshot reconciliation failed")
        manifest = {
            "snapshot_manifest_version": 1,
            "snapshot_id": self.directory.name,
            "capture_id": capture_manifest["capture_id"],
            "status": "completed",
            "capture": {
                "cluster_id": capture_manifest["cluster_id"],
                "topic": capture_manifest["topic"],
                "partition": capture_manifest["partition"],
                "start": capture_manifest["start"],
                "end": capture_manifest["end"],
                "segment_sha256": capture_manifest["segment"]["sha256"],
            },
            "counts": counts,
            "reconciliation": {
                "formula": "physical = exported + duplicates + transport_quarantine + contract_quarantine",
                "passed": True,
            },
            "outputs": {
                "events": "events.jsonl",
                "quarantine": "quarantine.jsonl",
                "manifest": "manifest.json",
            },
            "events_sha256": digest(events_path),
            "quarantine_sha256": digest(quarantine_path),
            "finished_at": utc_now(),
        }
        atomic_json(self.directory / "manifest.json", manifest)
        return manifest
