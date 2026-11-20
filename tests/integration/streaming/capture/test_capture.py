"""Real Kafka capture and replay acceptance for STREAM-004."""

import json
import os
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

from confluent_kafka.admin import AdminClient, NewTopic
import pytest

from pixguard_ai.application.capture_events import CaptureRequest, capture_events
from pixguard_ai.application.generate_events import GenerationConfig, generate_events
from pixguard_ai.application.publish_events import publish_events
from pixguard_ai.infrastructure.kafka.capture import KafkaCaptureSource
from pixguard_ai.infrastructure.kafka.capture_storage import CaptureStorage
from pixguard_ai.infrastructure.kafka.producer import KafkaPublisher
from pixguard_ai.infrastructure.kafka.storage import FilePublicationStore, JsonlPublicationSource
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.infrastructure.synthetic.jsonl import JsonlEventWriter


@pytest.fixture
def topic():
    """Create and clean one isolated real Kafka topic."""
    bootstrap = os.environ.get("PIXGUARD_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    name = "stream004-" + uuid4().hex
    admin.create_topics([NewTopic(name, 1, 1, config={
        "retention.ms": "86400000", "cleanup.policy": "delete",
    })])[name].result(20)
    deadline = monotonic() + 20
    while monotonic() < deadline:
        metadata = admin.list_topics(timeout=5).topics.get(name)
        if metadata and not metadata.error and 0 in metadata.partitions:
            partition = metadata.partitions[0]
            if partition.leader >= 0 and partition.isrs:
                break
        sleep(0.1)
    else:
        pytest.fail("isolated capture topic did not become ready")
    yield bootstrap, name, admin
    admin.delete_topics([name])[name].result(20)


def test_capture_preserves_bytes_coordinates_and_replay(tmp_path: Path, topic) -> None:
    """Ten published events can be captured, replayed and checksum-verified."""
    bootstrap, name, _ = topic
    events = tmp_path / "events.jsonl"
    generate_events(GenerationConfig(seed=42, count=10), SyntheticEventSource(), JsonlEventWriter(events))
    publications = FilePublicationStore(tmp_path / "publications")
    published = publish_events(
        "capture-source", JsonlPublicationSource(events, publications),
        KafkaPublisher(bootstrap, name), publications)
    assert published["status"] == "completed"
    original = events.read_bytes().splitlines()

    source = KafkaCaptureSource(bootstrap, "capture-first", name)
    request = CaptureRequest(
        "capture-first", source.identity(), name, 0, 0, 10, tmp_path)
    manifest = capture_events(request, source, CaptureStorage(tmp_path))
    source.close()

    segment = tmp_path / "captures" / "capture-first" / "records.jsonl"
    records = [json.loads(line) for line in segment.read_text().splitlines()]
    assert manifest["count"] == 10
    assert [record["offset"] for record in records] == list(range(10))
    for ordinal, record in enumerate(records):
        assert record["topic"] == name
        assert record["partition"] == 0
        assert __import__("base64").b64decode(record["value_base64"]) == original[ordinal]

    replay_source = KafkaCaptureSource(bootstrap, "capture-replay", name)
    replay_request = CaptureRequest(
        "capture-replay", replay_source.identity(), name, 0, 0, 10, tmp_path)
    replay = capture_events(replay_request, replay_source, CaptureStorage(tmp_path))
    replay_source.close()
    replay_segment = tmp_path / "captures" / "capture-replay" / "records.jsonl"
    replay_records = [json.loads(line) for line in replay_segment.read_text().splitlines()]
    assert replay["status"] == "completed"
    assert replay_records == records
