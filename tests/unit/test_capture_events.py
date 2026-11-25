"""Unit coverage for bounded, resumable Kafka capture accounting."""

import json
import os
from pathlib import Path

import pytest

from pixguard_ai.application.capture_events import CaptureRequest, capture_events
from pixguard_ai.infrastructure.kafka.capture_storage import CaptureStorage
from pixguard_ai.interfaces.cli import capture_output_root


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


def test_cli_capture_default_is_data_root(monkeypatch) -> None:
    monkeypatch.setenv("PIXGUARD_DATA_DIR", "/data/pixguard")
    assert capture_output_root() == Path("/data/pixguard")


def test_capture_accepts_null_key_and_value(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    storage.reserve(request(tmp_path, 1))
    storage.append({
        "topic": "pixguard.payment_event_v1", "partition": 0, "offset": 0,
        "key_base64": None, "value_base64": None,
        "headers": [["type", None]], "timestamp": 1700000000000,
    })
    manifest = storage.complete(1)
    assert manifest["count"] == 1


def test_partial_jsonl_tail_is_truncated_on_resume(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    storage.reserve(request(tmp_path, 4))
    storage.append({
        "topic": "pixguard.payment_event_v1", "partition": 0, "offset": 0,
        "key_base64": "a2V5", "value_base64": "dmFsdWU=",
        "headers": [], "timestamp": 1700000000000,
    })
    storage.lock.unlink(missing_ok=True)
    with storage.segment.open("ab") as stream:
        stream.write(b'{"offset":')
        stream.flush()
        os.fsync(stream.fileno())
    resumed = CaptureStorage(tmp_path)
    state = resumed.reserve(request(tmp_path, 4))
    assert state["next_offset"] == 1


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


def test_lock_symlink_is_rejected(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    base = tmp_path / "captures"
    base.mkdir()
    external = tmp_path / "external-lock"
    external.write_text("unchanged", encoding="utf-8")
    (base / ".capture-1.lock").symlink_to(external)
    with pytest.raises(ValueError, match="lock is a symlink"):
        storage.reserve(request(tmp_path, 4))
    assert external.read_text() == "unchanged"


def test_append_retries_short_writes_and_persists_full_record(tmp_path: Path, monkeypatch) -> None:
    storage = CaptureStorage(tmp_path)
    storage.reserve(request(tmp_path, 4))
    real_write = os.write
    calls = {"short": True}

    def short_then_full(descriptor: int, data: bytes) -> int:
        if calls["short"]:
            calls["short"] = False
            count = min(8, len(data))
            real_write(descriptor, data[:count])
            return count
        return real_write(descriptor, data)

    monkeypatch.setattr("pixguard_ai.infrastructure.kafka.capture_storage.os.write", short_then_full)
    storage.append({
        "topic": "pixguard.payment_event_v1", "partition": 0, "offset": 0,
        "key_base64": "a2V5", "value_base64": "dmFsdWU=",
        "headers": [], "timestamp": 1700000000000,
    })
    lines = storage.segment.read_bytes().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["offset"] == 0


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


def test_segment_validation_streams_without_read_bytes(tmp_path: Path, monkeypatch) -> None:
    storage = CaptureStorage(tmp_path)
    capture_events(request(tmp_path), FakeSource(), storage)
    manifest_path = storage.directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["status"] = "capturing"
    manifest_path.write_text(json.dumps(manifest))
    original = Path.read_bytes

    def bounded_read(path: Path) -> bytes:
        assert path.name != "records.jsonl", "segment must be streamed"
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", bounded_read)
    resumed = CaptureStorage(tmp_path)
    state = resumed.reserve(request(tmp_path))
    assert state["count"] == 4
    resumed.complete(4)
    assert resumed.verify_replay(request(tmp_path).topic, 0, 0, 4)["count"] == 4


@pytest.mark.parametrize("fail_commit", [False, True])
@pytest.mark.parametrize("batch_size", [1, 3])
def test_kafka_capture_renews_inactivity_and_confirms_after_commit(
    tmp_path: Path, monkeypatch, fail_commit: bool, batch_size: int,
) -> None:
    from pixguard_ai.infrastructure.kafka import capture as adapter

    monkeypatch.setattr(adapter, "COMMIT_BATCH_SIZE", batch_size)
    clock = [0.0]
    events = []

    class Message:
        def __init__(self, offset):
            self.number = offset

        def error(self):
            return None

        def offset(self):
            return self.number

        def topic(self):
            return request(tmp_path).topic

        def partition(self):
            return 0

        def key(self):
            return b"key"

        def value(self):
            return b"value"

        def headers(self):
            return []

        def timestamp(self):
            return (0, 1700000000000)

    class Consumer:
        next_offset = 0

        def assign(self, partitions):
            pass

        def poll(self, timeout):
            clock[0] += 20
            message = Message(self.next_offset)
            self.next_offset += 1
            return message

        def commit(self, message, asynchronous):
            events.append(("commit", message.offset()))
            return None if fail_commit else [adapter.TopicPartition(
                request(tmp_path).topic, 0, message.offset() + 1)]

    class Writer:
        def __call__(self, record):
            events.append(("append", record["offset"]))

        def confirmed(self, offset):
            events.append(("confirmed", offset))

    monkeypatch.setattr(adapter.time, "monotonic", lambda: clock[0])
    source = adapter.KafkaCaptureSource.__new__(adapter.KafkaCaptureSource)
    source.topic = request(tmp_path).topic
    source.partition = 0
    source._consumer = Consumer()
    if fail_commit:
        with pytest.raises(RuntimeError, match="not confirmed"):
            source.capture(0, 4, Writer())
        assert events == [("append", offset) for offset in range(batch_size)] + [
            ("commit", batch_size - 1)]
    else:
        assert source.capture(0, 4, Writer()) == 4
        assert clock[0] > 30
        expected = []
        for offset in range(4):
            expected.append(("append", offset))
            if (offset + 1) % batch_size == 0 or offset == 3:
                expected.extend([("commit", offset), ("confirmed", offset)])
        assert events == expected


def test_kafka_capture_stalled_broker_has_bounded_wait(monkeypatch) -> None:
    from pixguard_ai.infrastructure.kafka import capture as adapter

    clock = [0.0]

    class Consumer:
        def assign(self, partitions):
            pass

        def poll(self, timeout):
            clock[0] += timeout
            return None

    monkeypatch.setattr(adapter.time, "monotonic", lambda: clock[0])
    source = adapter.KafkaCaptureSource.__new__(adapter.KafkaCaptureSource)
    source.topic = "events"
    source.partition = 0
    source._consumer = Consumer()
    with pytest.raises(ValueError, match="timed out"):
        source.capture(0, 4, lambda record: None)
    assert clock[0] == 30


@pytest.mark.parametrize("failure", ["commit", "fsync"])
def test_failed_batch_commit_resumes_after_durable_records(tmp_path: Path, monkeypatch, failure: str) -> None:
    from unittest.mock import Mock
    from pixguard_ai.infrastructure.kafka import capture as adapter

    monkeypatch.setattr(adapter, "COMMIT_BATCH_SIZE", 3)
    source = adapter.KafkaCaptureSource.__new__(adapter.KafkaCaptureSource)
    source.topic = request(tmp_path).topic
    source.partition = 0
    source.identity = lambda: "cluster-1"
    source.watermarks = lambda partition: (0, 4)
    source.committed = lambda partition: None
    messages = []
    for offset in range(3):
        message = Mock()
        message.error.return_value = None
        message.offset.return_value = offset
        message.topic.return_value = source.topic
        message.partition.return_value = 0
        message.key.return_value = b"key"
        message.value.return_value = b"value"
        message.headers.return_value = []
        message.timestamp.return_value = (0, 1700000000000)
        messages.append(message)
    source._consumer = Mock()
    source._consumer.poll.side_effect = messages
    source._consumer.commit.return_value = None
    storage = CaptureStorage(tmp_path)
    original_fsync = os.fsync

    def fail_segment_fsync(descriptor: int) -> None:
        if (hasattr(storage, "segment") and storage.segment.exists()
                and os.fstat(descriptor).st_ino == storage.segment.stat().st_ino):
            raise OSError("injected batch fsync failure")
        original_fsync(descriptor)

    if failure == "fsync":
        monkeypatch.setattr(os, "fsync", fail_segment_fsync)
    with pytest.raises((RuntimeError, OSError), match="not confirmed|fsync failure"):
        capture_events(request(tmp_path), source, storage)
    if failure == "fsync":
        source._consumer.commit.assert_not_called()
    monkeypatch.setattr(os, "fsync", original_fsync)
    pending = json.loads((storage.directory / "manifest.json").read_text())
    assert "last_confirmed_offset" not in pending
    assert len(storage.segment.read_text().splitlines()) == 3
    result = capture_events(request(tmp_path), FakeSource(committed=0), CaptureStorage(tmp_path))
    assert result["count"] == 4
    assert [json.loads(line)["offset"] for line in storage.segment.read_text().splitlines()] == [0, 1, 2, 3]


def test_batch_syncs_segment_once_and_recovers_partial_tail(tmp_path: Path, monkeypatch) -> None:
    storage = CaptureStorage(tmp_path)
    storage.reserve(request(tmp_path))
    original = os.fsync
    synced = []

    def track_sync(descriptor: int) -> None:
        synced.append(os.fstat(descriptor).st_ino)
        original(descriptor)

    monkeypatch.setattr(os, "fsync", track_sync)
    records = [{"topic": request(tmp_path).topic, "partition": 0, "offset": offset,
                "key_base64": None, "value_base64": None, "headers": [], "timestamp": 0}
               for offset in range(3)]
    storage.append_batch(records)
    assert synced.count(storage.segment.stat().st_ino) == 1
    with storage.segment.open("ab") as stream:
        stream.write(b'{"offset":3')
    storage.close()
    resumed = CaptureStorage(tmp_path)
    state = resumed.reserve(request(tmp_path))
    assert state["count"] == 3
    assert state["next_offset"] == 3
    assert resumed.segment.read_bytes().endswith(b"\n")
    resumed.close()


@pytest.mark.parametrize("explicit_end", [None, 4])
def test_completed_capture_is_verified_without_manifest_mutation(tmp_path: Path, explicit_end) -> None:
    storage = CaptureStorage(tmp_path)
    original = capture_events(request(tmp_path), FakeSource(), storage)
    manifest_path = storage.directory / "manifest.json"
    before = manifest_path.read_bytes()
    before_stat = manifest_path.stat()
    # Completed replay reads its durable range even after broker retention advances.
    result = capture_events(request(tmp_path, explicit_end), FakeSource(low=4, high=8),
                            CaptureStorage(tmp_path))
    assert result == original
    assert manifest_path.read_bytes() == before
    assert manifest_path.stat().st_mtime_ns == before_stat.st_mtime_ns
    assert manifest_path.stat().st_ino == before_stat.st_ino


def test_completed_capture_corruption_is_rejected_without_rewriting_manifest(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    capture_events(request(tmp_path), FakeSource(), storage)
    manifest_path = storage.directory / "manifest.json"
    before = manifest_path.read_bytes()
    with storage.segment.open("ab") as stream:
        stream.write(b"{}\n")
    with pytest.raises(ValueError, match="checksum mismatch"):
        capture_events(request(tmp_path), FakeSource(), CaptureStorage(tmp_path))
    assert manifest_path.read_bytes() == before


def test_interrupted_capture_with_full_durable_range_still_completes(tmp_path: Path) -> None:
    storage = CaptureStorage(tmp_path)
    storage.reserve(request(tmp_path))
    FakeSource().capture(0, 4, storage.append)
    storage.close()
    pending = json.loads((storage.directory / "manifest.json").read_text())
    assert pending["status"] == "capturing"
    result = capture_events(request(tmp_path), FakeSource(), CaptureStorage(tmp_path))
    assert result["status"] == "completed"
    assert result["count"] == 4


@pytest.mark.parametrize("response", [None, [], "ok", [object()],
    [__import__('confluent_kafka').TopicPartition('wrong', 0, 3)],
    [__import__('confluent_kafka').TopicPartition('events', 1, 3)],
    [__import__('confluent_kafka').TopicPartition('events', 0, 2)]])
def test_commit_requires_exact_partition_confirmation(response) -> None:
    """Malformed or unrelated broker acknowledgements never advance manifest."""
    from unittest.mock import Mock
    from pixguard_ai.infrastructure.kafka.capture import KafkaCaptureSource

    source = KafkaCaptureSource.__new__(KafkaCaptureSource)
    source.topic, source.partition = "events", 0
    source._consumer = Mock()
    source._consumer.commit.return_value = response
    message = Mock()
    message.offset.return_value = 2
    writer = Mock()
    with pytest.raises(RuntimeError):
        source._confirm(message, writer)
    writer.confirmed.assert_not_called()


def test_contended_lock_cleanup_does_not_mask_error(tmp_path: Path) -> None:
    """A failed reservation releases its descriptor without touching its owner."""
    owner = CaptureStorage(tmp_path)
    owner.reserve(request(tmp_path))
    contender = CaptureStorage(tmp_path)
    try:
        with pytest.raises(FileExistsError, match="already active"):
            capture_events(request(tmp_path), FakeSource(), contender)
        contender.close()
    finally:
        owner.close()
    contender.reserve(request(tmp_path))
    contender.close()


def test_resume_rejects_modified_durable_record(tmp_path: Path) -> None:
    """An interrupted segment must retain its original record checksums."""
    storage = CaptureStorage(tmp_path)
    storage.reserve(request(tmp_path))
    FakeSource().capture(0, 4, storage.append)
    storage.close()
    lines = storage.segment.read_text().splitlines()
    record = json.loads(lines[0])
    record["timestamp"] += 1
    lines[0] = json.dumps(record)
    storage.segment.write_text("\n".join(lines) + "\n")
    with pytest.raises(ValueError, match="checksum mismatch"):
        capture_events(request(tmp_path), FakeSource(), CaptureStorage(tmp_path))
