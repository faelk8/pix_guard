"""Container-only Kafka client proof; opt in with PIXGUARD_STREAM001_PHASE."""

from hashlib import sha256
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from pixguard_ai.application.generate_events import GenerationConfig
from pixguard_ai.domain.events import PaymentEvent
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource

PHASE = os.environ.get("PIXGUARD_STREAM001_PHASE")
pytestmark = pytest.mark.skipif(PHASE not in {"publish", "replay"}, reason="isolated PoC opt-in")
RECEIPTS = Path("/data/pixguard/stream001-receipts.json")
BOOTSTRAP = "kafka:9092"


def source_records() -> list[dict[str, object]]:
    """Build deterministic canonical bytes and transport headers from ten events."""
    events = list(SyntheticEventSource().events(GenerationConfig(count=10, seed=42)))
    payloads = [json.dumps(event.to_dict(), separators=(",", ":"), sort_keys=True) for event in events]
    digest = sha256("\n".join(payloads).encode()).hexdigest()
    return [{"key": str(event.event_id), "value": payload, "headers": [
        ["source_batch_id", "stream001-seed42"], ["source_sha256", digest],
        ["source_ordinal", str(index)], ["transport_version", "1"],
    ]} for index, (event, payload) in enumerate(zip(events, payloads, strict=True))]


def publish() -> dict[str, object]:
    """Create a unique isolated topic and retain only confirmed delivery receipts."""
    topic = "pixguard.stream001." + uuid4().hex
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1,
                                    config={"cleanup.policy": "delete", "retention.ms": "86400000"})],
                        request_timeout=15)[topic].result(timeout=20)
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True,
                         "acks": "all", "delivery.timeout.ms": 10000})
    confirmed: list[dict[str, object]] = []
    errors: list[int] = []

    def delivered(error: object, message: object) -> None:
        """Separate failed callbacks from durable broker acknowledgements."""
        if error is not None:
            errors.append(error.code())
        else:
            confirmed.append({"topic": message.topic(), "partition": message.partition(),
                              "offset": message.offset(), "key": message.key().decode()})

    records = source_records()
    for record in records:
        producer.produce(
            topic, partition=0, key=record["key"].encode(), value=record["value"].encode(),
            headers=[(key, value.encode()) for key, value in record["headers"]], callback=delivered,
        )
    assert producer.flush(15) == 0, "unresolved deliveries"
    assert errors == []
    assert len(confirmed) == len(records) == 10
    assert [receipt["offset"] for receipt in confirmed] == list(range(10))
    assert [receipt["key"] for receipt in confirmed] == [record["key"] for record in records]
    evidence = {"topic": topic, "start": 0, "end": 10, "receipts": confirmed}
    RECEIPTS.write_text(json.dumps(evidence), encoding="utf-8")
    return evidence


def test_publish_or_explicit_replay() -> None:
    """Check bytes, canonical contract, keys, headers and receipt coordinates."""
    evidence = publish() if PHASE == "publish" else json.loads(RECEIPTS.read_text())
    topic = evidence["topic"]
    assert topic.startswith("pixguard.stream001.")
    records = source_records()
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "pixguard.stream001." + uuid4().hex,
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "auto.offset.reset": "error", "allow.auto.create.topics": False})
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10, cached=False)
        assert low <= evidence["start"] < evidence["end"] <= high
        assert high == 10, "replay must not republish or silently append"
        consumer.assign([TopicPartition(topic, 0, evidence["start"])])
        received = []
        deadline = time.monotonic() + 20
        while len(received) < 10 and time.monotonic() < deadline:
            message = consumer.poll(min(1, max(0, deadline - time.monotonic())))
            if message is None:
                continue
            assert message.error() is None, "Kafka read failed"
            index = len(received)
            expected = records[index]
            receipt = evidence["receipts"][index]
            assert (message.topic(), message.partition(), message.offset()) == (
                receipt["topic"], receipt["partition"], receipt["offset"])
            assert message.key() == expected["key"].encode()
            assert message.value() == expected["value"].encode()
            assert message.headers() == [(key, value.encode()) for key, value in expected["headers"]]
            decoded = json.loads(message.value())
            assert PaymentEvent.from_dict(decoded).to_dict() == decoded
            received.append(message.offset())
        assert received == list(range(evidence["start"], evidence["end"]))
        print(json.dumps({"phase": PHASE, "topic": topic, "count": len(received),
                          "partition": 0, "offsets": received, "contract": "payment_event_v1"}))
    finally:
        consumer.close()


def test_connection_failure_has_no_success() -> None:
    """A closed local endpoint must produce a bounded failed delivery callback."""
    callbacks: list[tuple[object, object]] = []
    producer = Producer({"bootstrap.servers": "127.0.0.1:1", "enable.idempotence": True,
                         "acks": "all", "delivery.timeout.ms": 2000, "socket.timeout.ms": 1000,
                         "socket.connection.setup.timeout.ms": 1000, "log_level": 0})
    started = time.monotonic()
    producer.produce("pixguard.stream001.unavailable", key=b"probe", value=b"probe",
                     callback=lambda error, message: callbacks.append(
                         (error, None if message is None else message.offset())))
    assert producer.flush(6) == 0
    elapsed = time.monotonic() - started
    assert len(callbacks) == 1
    assert callbacks[0][0] is not None
    assert callbacks[0][0].code() == KafkaError._MSG_TIMED_OUT
    assert callbacks[0][1] is None
    assert elapsed < 7
    print(json.dumps({"connection_failure": "_MSG_TIMED_OUT", "successes": 0,
                      "elapsed_seconds": round(elapsed, 3)}))
