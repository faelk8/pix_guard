"""Unit coverage for bounded, resumable Kafka capture accounting."""

import json
from pathlib import Path

import pytest

from pixguard_ai.application.capture_events import CaptureRequest, capture_events
from pixguard_ai.infrastructure.kafka.capture_storage import CaptureStorage


class FakeSource:
    def __init__(self, low: int = 0, high: int = 4, cluster: str = "cluster-1",
                 committed: int | None = None) -> None:
        self.low = low
        self.high = high
        self.cluster = cluster
        self.committed_offset = committed

    def identity(self) -> str:
        return self.cluster

    def watermarks(self, partition: int) -> tuple[int, int]:
        return self.low, self.high

    def committed(self, partition: int) -> int | None:
        return self.committed_offset

    def capture(self, start: int, end: int, on_record) -> int:
        for offset in range(start, end):
            on_record({
                "topic": "pixguard.payment_event_v1", "partition": 0, "offset": offset,
                "key_base64": "a2V5", "value_base64": "dmFsdWU=",
                "headers": [["transport_version", "MQ=="]], "timestamp": 1700000000000,
            })
        return end - start

    def close(self) -> None:
        return None


class FailingSource(FakeSource):
    def capture(self, start: int, end: int, on_record) -> int:
        raise RuntimeError("capture interrupted")


class EmptySource(FakeSource):
    def capture(self, start: int, end: int, on_record) -> int:
        return 0


def request(root: Path, end: int | None = 4) -> CaptureRequest:
    return CaptureRequest("capture-1", "cluster-1", "pixguard.payment_event_v1",
                          0, 0, end, root)


def test_capture_fixed_range_is_durable_and_replayable(tmp_path: Path) -> None:
    manifest = capture_events(request(tmp_path), FakeSource(), CaptureStorage(tmp_path))

    assert manifest["status"] == "completed"
    assert manifest["count"] == 4
    assert manifest["end"] == 4
    lines = (tmp_path / "captures" / "capture-1" / "records.jsonl").read_text().splitlines()
    assert [json.loads(line)["offset"] for line in lines] == [0, 1, 2, 3]


def test_capture_resume_finishes_contiguous_segment(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    state = storage.reserve(request(tmp_path, 4))
    storage.append({
        "topic": "pixguard.payment_event_v1", "partition": 0, "offset": 0,
        "key_base64": "a2V5", "value_base64": "dmFsdWU=",
        "headers": [], "timestamp": 1700000000000,
    })
    storage.lock.unlink(missing_ok=True)
    state = storage.reserve(request(tmp_path, 4))
    storage.lock.unlink(missing_ok=True)
    manifest = capture_events(request(tmp_path), FakeSource(), CaptureStorage(tmp_path))

    assert manifest["count"] == 4
    assert state["next_offset"] == 1


def test_sparse_offsets_are_physical_and_preserve_count(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    storage.reserve(request(tmp_path, 4))
    for offset in (0, 2):
        storage.append({
            "topic": "pixguard.payment_event_v1", "partition": 0, "offset": offset,
            "key_base64": "a2V5", "value_base64": "dmFsdWU=",
            "headers": [], "timestamp": 1700000000000,
        })
    manifest = storage.complete(2)
    assert manifest["count"] == 2
    assert [json.loads(line)["offset"] for line in
            (tmp_path / "captures" / "capture-1" / "records.jsonl").read_text().splitlines()] == [0, 2]


def test_resolved_end_is_not_recalculated(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    req = CaptureRequest("capture-end", "cluster-1", "pixguard.payment_event_v1",
                         0, 0, None, tmp_path)
    storage.reserve(req)
    storage.fix_end(3)
    storage.close()
    state = storage.reserve(req)
    assert state["fixed_end"] == 3


def test_capture_without_end_freezes_high_watermark(tmp_path: Path) -> None:
    req = CaptureRequest("capture-open", "cluster-1", "pixguard.payment_event_v1",
                         0, 0, None, tmp_path)
    manifest = capture_events(req, FakeSource(high=4), CaptureStorage(tmp_path))
    assert manifest["end"] == 4


def test_segment_symlink_is_rejected_without_external_write(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    storage.reserve(request(tmp_path, 4))
    external = tmp_path / "external.jsonl"
    external.write_text("unchanged\n", encoding="utf-8")
    storage.segment.symlink_to(external)
    with pytest.raises(ValueError, match="symlink"):
        storage.append({
            "topic": "pixguard.payment_event_v1", "partition": 0, "offset": 0,
            "key_base64": "a2V5", "value_base64": "dmFsdWU=",
            "headers": [], "timestamp": 1700000000000,
        })
    assert external.read_text() == "unchanged\n"


def test_empty_physical_range_materializes_empty_segment(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    manifest = capture_events(request(tmp_path, 4), EmptySource(high=4), storage)
    segment = tmp_path / "captures" / "capture-1" / "records.jsonl"
    assert manifest["status"] == "completed"
    assert manifest["count"] == 0
    assert segment.exists()
    assert segment.stat().st_size == 0


def test_capture_failure_releases_lock_for_same_process_retry(tmp_path: Path) -> None:
    req = request(tmp_path, 4)
    storage = CaptureStorage(tmp_path)
    with pytest.raises(RuntimeError, match="capture interrupted"):
        capture_events(req, FailingSource(), storage)
    retry = CaptureStorage(tmp_path)
    manifest = capture_events(req, FakeSource(), retry)
    assert manifest["status"] == "completed"


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (FakeSource(cluster="other"), "cluster identity mismatch"),
        (FakeSource(low=2), "low watermark"),
        (FakeSource(high=3), "high watermark"),
        (FakeSource(committed=2), "group is already ahead"),
    ],
)
def test_capture_rejects_identity_and_offset_safety(tmp_path: Path,
                                                   source: FakeSource,
                                                   message: str) -> None:
    with pytest.raises(ValueError, match=message):
        try:
            capture_events(request(tmp_path, 4), source, CaptureStorage(tmp_path))
        except ValueError as error:
            assert message in str(error)
            raise
