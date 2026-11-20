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
