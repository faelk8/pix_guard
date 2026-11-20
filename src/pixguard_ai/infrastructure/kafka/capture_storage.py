"""Durable segment and manifest storage for Kafka captures."""

import base64
from datetime import datetime, timezone
import fcntl
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any

from pixguard_ai.application.capture_events import CaptureRequest


CAPTURE_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
MAX_METADATA = 262_144


def canonical(value: object) -> bytes:
    """Encode bounded canonical JSON."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def utc_now() -> str:
    """Return an explicit UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate JSON keys."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def read_json(path: Path) -> dict[str, Any]:
    """Read a bounded JSON object."""
    if path.is_symlink():
        raise ValueError("capture metadata is a symlink")
    payload = path.read_bytes()
    if len(payload) > MAX_METADATA:
        raise ValueError("capture metadata exceeds limit")
    value = json.loads(payload, object_pairs_hook=unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    if not isinstance(value, dict):
        raise ValueError("capture metadata must be an object")
    return value


def atomic_json(path: Path, value: object) -> None:
    """Fsync JSON before atomic rename."""
    payload = canonical(value)
    if len(payload) > MAX_METADATA:
        raise ValueError("capture metadata exceeds limit")
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def sync_directory(path: Path) -> None:
    """Fsync a directory entry after file creation or rename."""
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


class CaptureStorage:
    """Local durable storage with exclusive capture reservation."""

    def __init__(self, data_root: Path) -> None:
        """Require an existing, non-symlink data root."""
        if data_root.is_symlink():
            raise ValueError("capture data root is a symlink")
        data_root.mkdir(parents=True, exist_ok=True)
        self.root = data_root.resolve(strict=True)

    def reserve(self, request: CaptureRequest) -> dict[str, Any]:
        """Reserve or resume only a matching capture identity and range."""
        if not CAPTURE_ID.fullmatch(request.capture_id):
            raise ValueError("invalid capture_id")
        if not isinstance(request.cluster_id, str) or not request.cluster_id:
            raise ValueError("missing cluster identity")
        if not isinstance(request.topic, str) or not request.topic or request.topic.startswith("."):
            raise ValueError("invalid Kafka topic")
        if type(request.partition) is not int or request.partition < 0:
            raise ValueError("invalid Kafka partition")
        if type(request.start) is not int or request.start < 0:
            raise ValueError("invalid Kafka start offset")
        if request.end is not None and (type(request.end) is not int or request.end <= request.start):
            raise ValueError("invalid Kafka end offset")

        base = self.root / "captures"
        if base.is_symlink():
            raise ValueError("capture namespace is a symlink")
        base.mkdir(mode=0o750, exist_ok=True)
        sync_directory(self.root)
        sync_directory(base)
        self.directory = base / request.capture_id
        if self.directory.is_symlink():
            raise ValueError("capture directory is a symlink")
        self.directory.mkdir(mode=0o750, exist_ok=True)
        sync_directory(base)
        self.lock = base / f".{request.capture_id}.lock"
        self._lock_fd = os.open(self.lock, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self._lock_fd)
            raise FileExistsError("capture is already active")
        self.segment = self.directory / "records.jsonl"
        self.request = request

        manifest_path = self.directory / "manifest.json"
        if manifest_path.exists():
            manifest = read_json(manifest_path)
            identity = (manifest.get("cluster_id"), manifest.get("topic"),
                        manifest.get("partition"), manifest.get("start"))
            expected = (request.cluster_id, request.topic, request.partition, request.start)
            if identity != expected:
                raise ValueError("capture identity or range mismatch")
            if manifest.get("status") == "completed":
                if manifest.get("end") != request.end and request.end is not None:
                    raise ValueError("completed capture range mismatch")
                self.close()
                return {"next_offset": manifest["end"], "count": manifest["count"],
                        "fixed_end": manifest["end"]}
            if manifest.get("status") != "capturing":
                raise ValueError("unsupported capture manifest status")

        else:
            pending = {
                "capture_manifest_version": 1, "capture_id": request.capture_id,
                "cluster_id": request.cluster_id, "topic": request.topic,
                "partition": request.partition, "start": request.start,
                "end": request.end, "status": "capturing", "started_at": utc_now(),
            }
            atomic_json(manifest_path, pending)

        next_offset = request.start
        count = 0
        if self.segment.exists():
            seen: list[int] = []
            for line in self.segment.read_bytes().splitlines():
                record = json.loads(line, object_pairs_hook=unique_object)
                if record["partition"] != request.partition or record["topic"] != request.topic:
                    raise ValueError("capture segment identity mismatch")
                seen.append(record["offset"])
            if seen:
                if seen != sorted(seen) or len(set(seen)) != len(seen):
                    raise ValueError("capture segment offsets are not ordered and unique")
                if any(offset < request.start for offset in seen):
                    raise ValueError("capture segment offset is below start")
                next_offset = seen[-1] + 1
                count = len(seen)
        fixed_end = manifest_path.exists()
        fixed_end = read_json(manifest_path).get("end") if fixed_end else request.end
        return {"next_offset": next_offset, "count": count, "fixed_end": fixed_end}

    def append(self, record: dict[str, Any]) -> None:
        """Append and fsync one raw record before Kafka offset commit."""
        required = {"topic", "partition", "offset", "key_base64", "value_base64", "headers", "timestamp"}
        if not required <= record.keys():
            raise ValueError("capture record is incomplete")
        base64.b64decode(record["key_base64"], validate=True)
        base64.b64decode(record["value_base64"], validate=True)
        stored = dict(record)
        stored["checksum_sha256"] = sha256(canonical(record)).hexdigest()
        payload = canonical(stored)
        if self.segment.is_symlink():
            raise ValueError("capture segment is a symlink")
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(self.segment, flags, 0o640)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ValueError("capture segment is not a regular file")
            stream = os.fdopen(descriptor, "ab", buffering=0, closefd=False)
            try:
                stream.write(payload + b"\n")
                os.fsync(descriptor)
            finally:
                stream.close()
        finally:
            os.close(descriptor)
        sync_directory(self.directory)

    def fix_end(self, end: int) -> None:
        """Freeze the resolved upper bound before offering records to Kafka."""
        manifest_path = self.directory / "manifest.json"
        manifest = read_json(manifest_path)
        if manifest["end"] is not None and manifest["end"] != end:
            raise ValueError("capture end changed after reservation")
        manifest["end"] = end
        atomic_json(manifest_path, manifest)

    def close(self) -> None:
        """Release the advisory lock; crash/kernel close performs the same action."""
        if hasattr(self, "_lock_fd"):
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            del self._lock_fd

    def confirmed(self, offset: int) -> None:
        """Persist the confirmed offset after storage fsync."""
        manifest_path = self.directory / "manifest.json"
        manifest = read_json(manifest_path)
        manifest["last_confirmed_offset"] = offset
        atomic_json(manifest_path, manifest)

    def complete(self, count: int) -> dict[str, Any]:
        """Publish a completed manifest after offsets reconcile."""
        manifest_path = self.directory / "manifest.json"
        manifest = read_json(manifest_path)
        end = manifest["end"]
        if end is None:
            raise ValueError("fixed end offset was not resolved")
        if not self.segment.exists():
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(self.segment, flags, 0o640)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            sync_directory(self.directory)
        if self.segment.is_symlink():
            raise ValueError("capture segment is a symlink")
        offsets = []
        if self.segment.exists():
            for line in self.segment.read_bytes().splitlines():
                record = json.loads(line, object_pairs_hook=unique_object)
                offsets.append(record["offset"])
        if (len(offsets) != count or len(set(offsets)) != len(offsets)
                or any(offset < manifest["start"] or offset >= end for offset in offsets)):
            raise ValueError("captured offsets do not reconcile")
        manifest.update({
            "status": "completed", "end": end, "count": count,
            "finished_at": utc_now(), "segment": {
                "path": "records.jsonl", "sha256": digest(self.segment),
                "size_bytes": self.segment.stat().st_size,
            },
        })
        atomic_json(manifest_path, manifest)
        self.close()
        return manifest

    def verify_replay(self, topic: str, partition: int, start: int, end: int) -> dict[str, Any]:
        """Verify a completed capture for replay without mutating it."""
        manifest = read_json(self.directory / "manifest.json")
        if (manifest.get("status") != "completed" or manifest.get("topic") != topic
                or manifest.get("partition") != partition or manifest.get("start") != start
                or manifest.get("end") != end):
            raise ValueError("capture is not a completed replay match")
        segment = self.directory / "records.jsonl"
        recorded = manifest["segment"]
        if digest(segment) != recorded["sha256"] or segment.stat().st_size != recorded["size_bytes"]:
            raise ValueError("capture segment checksum mismatch")
        offsets = []
        for line in segment.read_bytes().splitlines():
            record = json.loads(line, object_pairs_hook=unique_object)
            if sha256(canonical({key: value for key, value in record.items()
                                  if key != "checksum_sha256"})).hexdigest() != record["checksum_sha256"]:
                raise ValueError("capture record checksum mismatch")
            offsets.append(record["offset"])
        if (len(set(offsets)) != len(offsets)
                or any(offset < start or offset >= end for offset in offsets)):
            raise ValueError("capture replay offsets do not reconcile")
        return manifest
