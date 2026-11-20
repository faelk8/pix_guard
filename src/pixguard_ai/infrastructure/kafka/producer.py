"""Confluent Kafka adapter with explicit topic inspection and bounded delivery."""

import re
from time import monotonic
from typing import Callable

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType


class KafkaPublisher:
    """Use idempotence within one session without promising cross-run uniqueness."""

    def __init__(self, bootstrap: str, topic: str, timeout: float = 60.0) -> None:
        """Delay producer allocation until existing topic configuration is checked."""
        if (not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", topic)
                or topic in (".", "..") or not 0 < timeout <= 3600):
            raise ValueError("Invalid Kafka topic or timeout")
        self.bootstrap = bootstrap
        self.topic = topic
        self.timeout = timeout
        self.producer: Producer | None = None
        self.cluster_id: str | None = None

    def inspect(self, timeout: float) -> dict[str, object]:
        """Read all metadata to avoid implicit creation of a named missing topic."""
        started = monotonic()
        admin = AdminClient({"bootstrap.servers": self.bootstrap, "log_level": 0})
        metadata = admin.list_topics(timeout=timeout)
        topic = metadata.topics.get(self.topic)
        if topic is None or topic.error or len(topic.partitions) != 1:
            raise ValueError("Kafka topic is missing or has incompatible topology")
        partition = topic.partitions.get(0)
        if partition is None or partition.error or len(partition.replicas) != 1:
            raise ValueError("Kafka replication must be one")
        remaining = timeout - (monotonic() - started)
        if remaining <= 0:
            raise TimeoutError("Topic inspection deadline exceeded")
        resource = ConfigResource(ResourceType.TOPIC, self.topic)
        configs = admin.describe_configs([resource], request_timeout=remaining)[resource].result(remaining)
        if any(configs[name].value != expected for name, expected in {
            "retention.ms": "86400000", "cleanup.policy": "delete",
        }.items()):
            raise ValueError("Kafka effective topic configuration mismatch")
        if not metadata.cluster_id:
            raise ValueError("Kafka cluster identity is missing")
        self.cluster_id = metadata.cluster_id
        self.producer = Producer({
            "bootstrap.servers": self.bootstrap, "enable.idempotence": True,
            "acks": "all", "delivery.timeout.ms": max(1000, int(self.timeout * 1000)),
            "request.timeout.ms": max(1000, min(30000, int(self.timeout * 1000))),
            "linger.ms": 0, "queue.buffering.max.messages": 1000,
            "message.max.bytes": 1048576, "log_level": 0,
        })
        return {"cluster_id": self.cluster_id, "topic": self.topic,
                "topic_configuration": {"partitions": 1, "replication_factor": 1,
                                        "retention.ms": "86400000", "cleanup.policy": "delete"}}

    def send(self, key: str, payload: bytes, headers: dict[str, str],
             callback: Callable[[dict[str, object] | None], None]) -> None:
        """Offer the unchanged event bytes and report only successful broker acks."""
        if self.producer is None:
            raise RuntimeError("Topic must be inspected before publishing")

        def delivered(error: object, message: object) -> None:
            """Do not expose client error strings, which can contain endpoints."""
            if error is not None:
                callback(None)
            else:
                callback({"cluster_id": self.cluster_id, "topic": message.topic(),
                          "partition": message.partition(), "offset": message.offset()})

        self.producer.produce(self.topic, partition=0, key=key.encode("utf-8"),
                              value=payload, headers=list(headers.items()), on_delivery=delivered)

    def flush(self, timeout: float) -> int:
        """Poll callbacks only within the remaining whole-publication deadline."""
        if self.producer is None:
            raise RuntimeError("Topic must be inspected before publishing")
        return self.producer.flush(timeout)
