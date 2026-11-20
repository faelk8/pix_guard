"""Filesystem publication checkpoints and validated private source snapshots."""

from hashlib import sha256
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Iterable

from pixguard_ai.application.publish_events import SourceInfo
from pixguard_ai.domain.events import PaymentEvent, SCHEMA_VERSION


def sync_directory(path: Path) -> None:
    """Make directory-entry changes durable on the local filesystem."""
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path: Path, value: object) -> None:
    """Publish a JSON document only after its bytes are durable."""
    with tempfile.NamedTemporaryFile(dir=path.parent, mode="w", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
            sync_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)


class FilePublicationStore:
    """Reserve IDs using atomic mkdir; never overwrite a previous execution."""

    def __init__(self, root: Path) -> None:
        """Choose the namespace in which batch identifiers are exclusive."""
        self.root = root
        self.directory: Path | None = None

    def reserve(self, batch_id: str) -> None:
        """Validate IDs before they become paths or transport headers."""
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", batch_id):
            raise ValueError("Invalid source batch ID")
        self.root.mkdir(parents=True, exist_ok=True)
        directory = self.root / batch_id
        directory.mkdir(mode=0o700)
        sync_directory(self.root)
        self.directory = directory

    def checkpoint(self, manifest: dict[str, object]) -> None:
        """Atomically replace the authoritative run state."""
        if self.directory is None:
            raise RuntimeError("Publication is not reserved")
        atomic_json(self.directory / "manifest.json", manifest)

    def receipts(self, window: int, records: list[dict[str, object]]) -> None:
        """Persist bounded receipt windows, including exact broker coordinates."""
        if self.directory is None:
            raise RuntimeError("Publication is not reserved")
        atomic_json(self.directory / f"receipts-{window:08d}.json", records)


class JsonlPublicationSource:
    """Publish from a private copy, never reopen the mutable generation output."""

    def __init__(self, path: Path, store: FilePublicationStore) -> None:
        """Bind the mutable generation path and the reserved publication store."""
        self.path = path
        self.store = store
        self.snapshot: Path | None = None
        self.info: SourceInfo | None = None

    def prepare(self) -> SourceInfo:
        """Validate the entire bounded-record copy before Kafka sees any record."""
        with self.path.with_suffix(".manifest.json").open("rb") as stream:
            manifest_bytes = stream.read(65537)
        if len(manifest_bytes) > 65536:
            raise ValueError("Source manifest exceeds size limit")
        manifest = json.loads(manifest_bytes)
        if (not isinstance(manifest, dict) or manifest.get("status") != "completed"
                or manifest.get("schema_version") != SCHEMA_VERSION
                or manifest.get("manifest_version") != 1
                or manifest.get("label_is_synthetic") is not True
                or type(manifest.get("count_requested")) is not int
                or type(manifest.get("count_written")) is not int
                or manifest["count_written"] < 1
                or manifest.get("count_requested") != manifest["count_written"]):
            raise ValueError("Source manifest is not a completed v1 batch")
        if self.store.directory is None:
            raise RuntimeError("Publication is not reserved")
        self.snapshot = self.store.directory / "source.jsonl"
        checksum = sha256()
        count = 0
        with self.path.open("rb") as origin, self.snapshot.open("xb") as target:
            while line := origin.readline(1024 * 1024 + 1):
                if len(line) > 1024 * 1024 or not line.endswith(b"\n"):
                    raise ValueError("Source record exceeds limit or is unterminated")
                if count >= manifest["count_written"]:
                    raise ValueError("Source exceeds declared count")
                self._decode(line)
                target.write(line)
                checksum.update(line)
                count += 1
            target.flush()
            os.fsync(target.fileno())
        self.snapshot.chmod(0o400)
        sync_directory(self.snapshot.parent)
        with self.path.with_suffix(".manifest.json").open("rb") as stream:
            final_manifest = stream.read(65537)
        if (checksum.hexdigest() != manifest.get("sha256")
                or count != manifest["count_written"]
                or manifest_bytes != final_manifest):
            raise ValueError("Source checksum, count or manifest changed")
        self.info = SourceInfo(count=count, sha256=checksum.hexdigest())
        return self.info

    @staticmethod
    def _decode(line: bytes) -> str:
        """Reject duplicate JSON fields and reuse the canonical event contract."""
        def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("Duplicate JSON field")
                result[key] = value
            return result
        event = PaymentEvent.from_dict(json.loads(line.decode("utf-8"), object_pairs_hook=unique))
        return str(event.event_id)

    def records(self) -> Iterable[tuple[str, bytes]]:
        """Yield exact JSON bytes, excluding the JSONL record delimiter."""
        if self.snapshot is None:
            raise RuntimeError("Source was not prepared")
        with self.snapshot.open("rb") as stream:
            for line in stream:
                yield self._decode(line), line[:-1]

    def verify(self) -> None:
        """Require the private snapshot checksum to remain intact after sending."""
        if self.snapshot is None or self.info is None:
            raise RuntimeError("Source was not prepared")
        checksum = sha256()
        with self.snapshot.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                checksum.update(chunk)
        if checksum.hexdigest() != self.info.sha256:
            raise ValueError("Private source checksum changed")
