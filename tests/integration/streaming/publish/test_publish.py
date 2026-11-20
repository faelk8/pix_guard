"""Isolated Kafka proof for the actual application and persisted wire receipts."""

import json
import os
from pathlib import Path
import subprocess
import sys
from time import monotonic, sleep
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, NewTopic, ResourceType
import pytest

from pixguard_ai.application.generate_events import GenerationConfig, generate_events
from pixguard_ai.application.publish_events import publish_events
from pixguard_ai.infrastructure.kafka.producer import KafkaPublisher
from pixguard_ai.infrastructure.kafka.storage import FilePublicationStore, JsonlPublicationSource
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.infrastructure.synthetic.jsonl import JsonlEventWriter


@pytest.fixture
def topic():
    bootstrap = os.environ.get("PIXGUARD_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    name = "stream003-" + uuid4().hex
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
        pytest.fail("Isolated topic did not become ready")
    yield bootstrap, name, admin
    admin.delete_topics([name])[name].result(20)


def source(tmp_path):
    path = tmp_path / "events.jsonl"
    generate_events(GenerationConfig(seed=42, count=10), SyntheticEventSource(), JsonlEventWriter(path))
    store = FilePublicationStore(tmp_path / "publications")
    return path, store, JsonlPublicationSource(path, store)


def test_real_ten_receipts_keys_headers_and_republication(tmp_path, topic):
    bootstrap, name, _ = topic
    path, store, events = source(tmp_path)
    result = publish_events("first", events, KafkaPublisher(bootstrap, name), store)
    assert result["status"] == "completed"
    assert (result["confirmed"], result["uncertain"], result["not_sent"]) == (10, 0, 0)
    receipts = json.loads((store.directory / "receipts-00000000.json").read_text())
    original = path.read_bytes().splitlines()
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": uuid4().hex,
                         "enable.auto.commit": False, "auto.offset.reset": "error"})
    consumer.assign([TopicPartition(name, 0, 0)])
    messages = []
    try:
        deadline = monotonic() + 20
        while len(messages) < 10 and monotonic() < deadline:
            message = consumer.poll(1)
            if message is not None:
                assert not message.error()
                messages.append(message)
    finally:
        consumer.close()
    assert len(messages) == 10
    for ordinal, (message, receipt) in enumerate(zip(messages, receipts, strict=True)):
        assert message.value() == original[ordinal]
        assert message.key().decode() == json.loads(original[ordinal])["event_id"]
        assert dict(message.headers()) == {
            "source_batch_id": b"first", "source_sha256": result["source_sha256"].encode(),
            "source_ordinal": str(ordinal).encode(), "transport_version": b"1",
        }
        assert (receipt["topic"], receipt["partition"], receipt["offset"]) == (
            message.topic(), message.partition(), message.offset())
        assert receipt["cluster_id"] == result["cluster_id"]
    before = (store.directory / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        publish_events("first", events, KafkaPublisher(bootstrap, name), store)
    assert (store.directory / "manifest.json").read_bytes() == before
    # A fresh session and ID is explicit republication, not exactly-once.
    second = FilePublicationStore(tmp_path / "publications")
    repeat = publish_events("second", JsonlPublicationSource(path, second), KafkaPublisher(bootstrap, name), second)
    assert repeat["confirmed"] == 10
    receipts2 = json.loads((second.directory / "receipts-00000000.json").read_text())
    assert [r["offset"] for r in receipts2] == list(range(10, 20))
    print("real Kafka: 10 intact payloads, keys, headers and coordinates; republication offsets 10..19")


def test_changed_effective_configuration_blocks_every_run(tmp_path, topic):
    bootstrap, name, admin = topic
    path, store, events = source(tmp_path)
    resource = ConfigResource(ResourceType.TOPIC, name, incremental_configs=[
        ConfigEntry("retention.ms", "120000", incremental_operation=AlterConfigOpType.SET),
    ])
    admin.incremental_alter_configs([resource])[resource].result(20)
    with pytest.raises(ValueError):
        publish_events("bad-config", events, KafkaPublisher(bootstrap, name), store)
    manifest = json.loads((store.directory / "manifest.json").read_text())
    assert manifest["status"] == "failed" and manifest["confirmed"] == 0
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": uuid4().hex})
    try:
        consumer.list_topics(timeout=10)
        assert consumer.get_watermark_offsets(TopicPartition(name, 0), timeout=10) == (0, 0)
    finally:
        consumer.close()


def test_missing_topic_is_not_created(tmp_path, topic):
    bootstrap, _, admin = topic
    _, store, events = source(tmp_path)
    missing = "missing-" + uuid4().hex
    with pytest.raises(ValueError):
        publish_events("missing", events, KafkaPublisher(bootstrap, missing), store)
    assert missing not in admin.list_topics(timeout=10).topics


def test_actual_queued_messages_without_ack_stay_uncertain(tmp_path, topic):
    bootstrap, name, _ = topic
    _, store, events = source(tmp_path)
    publisher = KafkaPublisher(bootstrap, name)
    # Simulate lost observation of acks with the actual producer queue. Later
    # delivery cannot retroactively turn the persisted failed run into success.
    publisher.flush = lambda timeout: 3
    with pytest.raises(TimeoutError):
        publish_events("lost-ack", events, publisher, store, timeout=10, window_size=3)
    manifest = json.loads((store.directory / "manifest.json").read_text())
    assert (manifest["confirmed"], manifest["uncertain"], manifest["not_sent"]) == (0, 3, 7)
    publisher.producer.flush(5)
    assert json.loads((store.directory / "manifest.json").read_text()) == manifest


def test_real_unavailable_broker_is_bounded(tmp_path):
    _, store, events = source(tmp_path)
    started = monotonic()
    with pytest.raises(Exception):
        publish_events("unavailable", events, KafkaPublisher("127.0.0.1:1", "missing", 1), store, timeout=1)
    assert monotonic() - started < 5
    manifest = json.loads((store.directory / "manifest.json").read_text())
    assert (manifest["confirmed"], manifest["uncertain"], manifest["not_sent"]) == (0, 0, 10)


def test_process_crash_leaves_durable_uncertainty(tmp_path, topic):
    bootstrap, name, _ = topic
    path, store, _ = source(tmp_path)
    script = '''
import os, sys
from pathlib import Path
from pixguard_ai.application.publish_events import publish_events
from pixguard_ai.infrastructure.kafka.producer import KafkaPublisher
from pixguard_ai.infrastructure.kafka.storage import FilePublicationStore, JsonlPublicationSource
store = FilePublicationStore(Path(sys.argv[2]))
publisher = KafkaPublisher(sys.argv[3], sys.argv[4])
original = publisher.send
def crash(*args):
    original(*args)
    os._exit(73)
publisher.send = crash
publish_events("crash", JsonlPublicationSource(Path(sys.argv[1]), store), publisher, store, window_size=3)
'''
    result = subprocess.run([sys.executable, "-c", script, str(path), str(store.root), bootstrap, name], timeout=30)
    assert result.returncode == 73
    manifest = json.loads((store.root / "crash" / "manifest.json").read_text())
    assert manifest["status"] == "publishing"
    assert (manifest["confirmed"], manifest["uncertain"], manifest["not_sent"]) == (0, 3, 7)
    with pytest.raises(FileExistsError):
        publish_events("crash", JsonlPublicationSource(path, store), KafkaPublisher(bootstrap, name), store)
