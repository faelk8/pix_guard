"""ADR 0006 acceptance against a disposable real Kafka broker."""

import errno
import json
from pathlib import Path
from uuid import uuid4
from typing import Any

from confluent_kafka import KafkaException, Producer, TopicPartition
import pytest

from pixguard_ai.application.capture_events import CaptureRequest, capture_events
from pixguard_ai.application.export_capture import ExportRequest, export_capture
from pixguard_ai.application.generate_events import GenerationConfig, generate_events
from pixguard_ai.infrastructure.kafka.capture import KafkaCaptureSource
from pixguard_ai.infrastructure.kafka.capture_storage import CaptureStorage
from pixguard_ai.infrastructure.local.capture_snapshot import CaptureSnapshot
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.infrastructure.synthetic.jsonl import JsonlEventWriter


def publish_fixture(root: Path, bootstrap: str, name: str) -> None:
    """Publish ten valid, duplicate, conflicting and malformed records."""
    path = root / "fixture.jsonl"
    generate_events(GenerationConfig(seed=42, count=11), SyntheticEventSource(), JsonlEventWriter(path))
    events = path.read_bytes().splitlines()
    conflict = json.loads(events[10])
    conflict["features"]["amount_brl"] = "123.45" if conflict["features"]["amount_brl"] != "123.45" else "234.56"
    values = events[:10] + [events[0], events[10], json.dumps(conflict).encode(), b"{invalid", b"\xff"]
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    errors = []
    for ordinal, value in enumerate(values):
        key = json.loads(value)["event_id"] if ordinal < 13 else "invalid"
        producer.produce(name, key=key, value=value, headers=[
            ("source_batch_id", b"fixture"), ("source_sha256", b"a" * 64),
            ("source_ordinal", str(ordinal).encode()), ("transport_version", b"1")],
            on_delivery=lambda error, message: errors.append(error) if error else None)
    assert producer.flush(20) == 0
    assert not errors


def capture(root: Path, bootstrap: str, name: str, capture_id: str, storage: CaptureStorage | None = None, source: KafkaCaptureSource | None = None) -> dict[str, Any]:
    """Close the consumer even when capture fails."""
    source = source or KafkaCaptureSource(bootstrap, capture_id, name)
    try:
        request = CaptureRequest(capture_id, source.identity(), name, 0, 0, 15, root)
        return capture_events(request, source, storage or CaptureStorage(root))
    finally:
        source.close()


def assert_complete(root: Path, capture_id: str) -> None:
    """Assert physical conservation and complete snapshot classification."""
    folder = root / "captures" / capture_id
    records = [json.loads(line) for line in folder.joinpath("records.jsonl").read_text().splitlines()]
    assert [record["offset"] for record in records] == list(range(15))
    storage = CaptureSnapshot(root)
    result = export_capture(ExportRequest(capture_id, "fixture", root), storage, storage)
    assert result["counts"] == {
        "physical": 15, "exported": 10, "duplicates": 1,
        "contract_quarantine": 3, "transport_quarantine": 1,
    }


def test_fixture_15(tmp_path: Path, topic) -> None:
    """All fifteen physical positions receive the ADR classification."""
    bootstrap, name, _ = topic
    publish_fixture(tmp_path, bootstrap, name)
    assert capture(tmp_path, bootstrap, name, "fixture")["count"] == 15
    assert_complete(tmp_path, "fixture")


@pytest.mark.parametrize("boundary", ["before_fsync", "after_fsync", "after_commit", "disk_full"])
def test_recovery_boundaries(tmp_path: Path, topic, boundary: str) -> None:
    """Inject bounded I/O interruption, then resume with the same real group."""
    bootstrap, name, _ = topic
    publish_fixture(tmp_path, bootstrap, name)
    capture_id = "recovery-" + uuid4().hex

    class InterruptedStorage(CaptureStorage):
        def append_batch(self, records: list[dict]) -> None:
            if boundary in ("before_fsync", "disk_full"):
                raise OSError(errno.ENOSPC if boundary == "disk_full" else errno.EIO, boundary)
            super().append_batch(records)
            if boundary == "after_fsync":
                raise OSError(errno.EIO, boundary)

    source = KafkaCaptureSource(bootstrap, capture_id, name)
    if boundary == "after_commit":
        original = source._consumer

        class InterruptedConsumer:
            def __getattr__(self, name: str) -> Any:
                return getattr(original, name)

            def commit(self, *args: Any, **kwargs: Any) -> Any:
                result = original.commit(*args, **kwargs)
                if kwargs["message"].offset() == 14:
                    raise OSError(errno.EIO, "after_commit")
                return result

        source._consumer = InterruptedConsumer()
    with pytest.raises(OSError):
        capture(tmp_path, bootstrap, name, capture_id, InterruptedStorage(tmp_path), source)
    assert capture(tmp_path, bootstrap, name, capture_id)["count"] == 15
    assert_complete(tmp_path, capture_id)


def test_expired_offset(tmp_path: Path, topic) -> None:
    """A real broker low watermark rejects a range whose prefix expired."""
    bootstrap, name, admin = topic
    publish_fixture(tmp_path, bootstrap, name)
    for future in admin.delete_records([TopicPartition(name, 0, 2)]).values():
        future.result(20)
    with pytest.raises(ValueError, match="low watermark"):
        capture(tmp_path, bootstrap, name, "expired")
    assert not (tmp_path / "captures" / "expired" / "records.jsonl").exists()


def test_broker_unavailable(tmp_path: Path) -> None:
    """An unreachable endpoint cannot produce a completed capture."""
    with pytest.raises((KafkaException, ValueError)):
        capture(tmp_path, "127.0.0.1:1", "unavailable", "unavailable")
    assert not (tmp_path / "captures" / "unavailable").exists()


def test_batch_verifier(tmp_path: Path, topic) -> None:
    """Exercise the receipt verifier on a clean cumulative publication."""
    import subprocess
    import sys
    from pixguard_ai.application.publish_events import publish_events
    from pixguard_ai.infrastructure.kafka.producer import KafkaPublisher
    from pixguard_ai.infrastructure.kafka.storage import FilePublicationStore, JsonlPublicationSource

    bootstrap, name, _ = topic
    events = tmp_path / "clean.jsonl"
    generate_events(GenerationConfig(seed=42, count=10), SyntheticEventSource(), JsonlEventWriter(events))
    publications = FilePublicationStore(tmp_path / "publications")
    for publication_id in ("first", "second"):
        result = publish_events(publication_id, JsonlPublicationSource(events, publications),
                               KafkaPublisher(bootstrap, name), publications)
        assert result["status"] == "completed"
    source = KafkaCaptureSource(bootstrap, "clean", name)
    try:
        capture_events(CaptureRequest("clean", source.identity(), name, 0, 0, 20, tmp_path),
                       source, CaptureStorage(tmp_path))
    finally:
        source.close()
    storage = CaptureSnapshot(tmp_path)
    export_capture(ExportRequest("clean", "clean", tmp_path), storage, storage)
    command = [sys.executable, str(Path(__file__).with_name("verify_batch.py")),
               "--source", str(events), "--capture", str(tmp_path / "captures" / "clean"),
               "--snapshot", str(tmp_path / "captures" / "clean" / "snapshots" / "clean"),
               "--expected-count", "10", "--expected-physical", "20"]
    receipt = subprocess.run(command, capture_output=True, text=True, check=True)
    assert json.loads(receipt.stdout)["status"] == "verified"
    events.write_text(events.read_text() + events.read_text().splitlines()[0] + "\n")
    assert subprocess.run(command, capture_output=True).returncode != 0
