"""Application use case for durable Kafka range capture."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class CaptureRequest:
    """Immutable request for one bounded, exclusive Kafka capture."""

    capture_id: str
    cluster_id: str
    topic: str
    partition: int
    start: int
    end: int | None
    data_root: Path


class CaptureSource(Protocol):
    """Kafka operations required by the capture use case."""

    def identity(self) -> str:
        """Return the broker cluster identity."""
        ...

    def watermarks(self, partition: int) -> tuple[int, int]:
        """Return the low and high offsets for the partition."""
        ...

    def committed(self, partition: int) -> int | None:
        """Return the committed offset for this capture group, if any."""
        ...

    def capture(self, start: int, end: int, on_record: Any) -> int:
        """Persist records through the callback until end and return count."""
        ...

    def close(self) -> None:
        """Close the Kafka consumer."""
        ...


class CaptureStorage(Protocol):
    """Durable storage operations required by the capture use case."""

    def reserve(self, request: CaptureRequest) -> dict[str, Any]:
        """Reserve or resume a capture directory."""
        ...

    def fix_end(self, end: int) -> None:
        """Freeze the resolved upper bound in storage."""
        ...

    def close(self) -> None:
        """Release the exclusive capture lock."""
        ...

    def append(self, record: dict[str, Any]) -> None:
        """Fsync one raw Kafka record before the Kafka offset is confirmed."""
        ...

    def confirmed(self, offset: int) -> None:
        """Persist the offset only after the record and manifest are durable."""
        ...

    def complete(self, count: int) -> dict[str, Any]:
        """Publish and return a verified completed manifest."""
        ...

    def verify_replay(self, topic: str, partition: int, start: int, end: int) -> dict[str, Any]:
        """Verify a completed capture for replay without mutating it."""
        ...


@dataclass(frozen=True)
class DurableCaptureWriter:
    """Keep append callable compatibility and expose post-commit confirmation."""

    storage: CaptureStorage

    def __call__(self, record: dict[str, Any]) -> None:
        """Make the record durable before the source commits its offset."""
        self.storage.append(record)

    def append_batch(self, records: list[dict[str, Any]]) -> None:
        """Durably append a source batch, falling back to individual storage."""
        append_batch = getattr(self.storage, "append_batch", None)
        if append_batch is not None:
            append_batch(records)
        else:
            for record in records:
                self.storage.append(record)

    def confirmed(self, offset: int) -> None:
        """Record successful broker confirmation after durable append."""
        self.storage.confirmed(offset)


def capture_events(request: CaptureRequest, source: CaptureSource, storage: CaptureStorage) -> dict[str, Any]:
    """Capture [start,end), validating cluster, offsets and group before and during writes."""
    if source.identity() != request.cluster_id:
        raise ValueError("Kafka cluster identity mismatch")

    try:
        state = storage.reserve(request)
    except BaseException:
        if hasattr(storage, "close"):
            storage.close()
        raise
    try:
        if state.get("status") == "completed":
            return storage.verify_replay(
                request.topic, request.partition, request.start, int(state["fixed_end"]))
        low, high = source.watermarks(request.partition)
        persisted_end = state.get("fixed_end")
        end = request.end if request.end is not None else (
            int(persisted_end) if persisted_end is not None else high)
        if hasattr(storage, "fix_end"):
            storage.fix_end(end)
        if end > high:
            raise ValueError("capture end exceeds available high watermark")
        if request.start < low:
            raise ValueError("capture start is before available low watermark")
        if request.start >= end:
            raise ValueError("capture range must be non-empty")

        resume_offset = int(state.get("next_offset", request.start))
        start = max(request.start, resume_offset)
        if start >= end:
            return storage.complete(int(state.get("count", 0)))
        committed = source.committed(request.partition)
        if committed is not None and committed > start:
            raise ValueError("capture consumer group is already ahead of requested start")
        count = source.capture(start, end, DurableCaptureWriter(storage))
        count += int(state.get("count", 0))
        return storage.complete(count)
    finally:
        if hasattr(storage, "close"):
            storage.close()
