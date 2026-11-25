"""Confluent Kafka adapter for bounded durable captures."""

from pathlib import Path
import time
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient


COMMIT_BATCH_SIZE = 100


class KafkaCaptureSource:
    """Read an assigned partition with manual offset storage and commit."""

    def __init__(self, bootstrap: str, capture_id: str, topic: str, partition: int = 0) -> None:
        """Configure a unique consumer group with auto commit and storage disabled."""
        self.topic = topic
        self.partition = partition
        self._admin = AdminClient({"bootstrap.servers": bootstrap})
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": f"pixguard.capture.{capture_id}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "error",
            "allow.auto.create.topics": False,
        })

    def identity(self) -> str:
        """Return cluster identity; fail closed when metadata is unavailable."""
        metadata = self._consumer.list_topics(self.topic, timeout=10)
        cluster_id = getattr(metadata, "cluster_id", None)
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ValueError("Kafka cluster identity unavailable")
        return cluster_id

    def watermarks(self, partition: int) -> tuple[int, int]:
        """Read the current low/high offsets without caching."""
        low, high = self._consumer.get_watermark_offsets(
            TopicPartition(self.topic, partition), timeout=10, cached=False)
        return int(low), int(high)

    def committed(self, partition: int) -> int | None:
        """Read the committed offset for this consumer group."""
        deadline = time.monotonic() + 20
        while True:
            try:
                assignment = self._consumer.committed(
                    [TopicPartition(self.topic, partition)], timeout=5)
                offset = assignment[0].offset
                break
            except KafkaException as error:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
        return None if offset is None or offset < 0 else int(offset)

    def capture(self, start: int, end: int, on_record: Callable[[dict[str, Any]], None]) -> int:
        """Poll exactly [start,end), calling the storage callback once per record."""
        self._consumer.assign([TopicPartition(self.topic, self.partition, start)])
        count = 0
        deadline = time.monotonic() + 30
        last_offset = start - 1
        pending = None
        batch = []
        append_batch = getattr(on_record, "append_batch", None)
        while time.monotonic() < deadline:
            message = self._consumer.poll(1)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    break
                raise RuntimeError(f"Kafka read failed: {error}")
            if message.offset() >= end:
                break
            if message.offset() <= last_offset:
                raise ValueError("Kafka capture offsets did not advance")
            key = message.key()
            value = message.value()
            headers = [(name, None if payload is None else bytes(payload))
                       for name, payload in (message.headers() or [])]
            record = {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "key_base64": None if key is None else __import__("base64").b64encode(key).decode("ascii"),
                "value_base64": None if value is None else __import__("base64").b64encode(value).decode("ascii"),
                "headers": [[name, None if payload is None else __import__("base64").b64encode(payload).decode("ascii")]
                            for name, payload in headers],
                "timestamp": message.timestamp()[1],
            }
            if append_batch is None:
                on_record(record)
            else:
                batch.append(record)
            pending = message
            if (count + 1) % COMMIT_BATCH_SIZE == 0:
                if append_batch is not None:
                    append_batch(batch)
                    batch.clear()
                self._confirm(pending, on_record)
                pending = None
            last_offset = message.offset()
            count += 1
            # A batch is fully fsynced before advancing the broker offset.
            # Reading progress renews the inactivity budget for large ranges.
            deadline = time.monotonic() + 30
        if time.monotonic() >= deadline:
            raise ValueError("Kafka capture timed out before fixed upper bound")
        if pending is not None:
            if append_batch is not None:
                append_batch(batch)
            self._confirm(pending, on_record)
        return count

    def _confirm(self, message: Any, on_record: Callable[[dict[str, Any]], None]) -> None:
        """Commit only a batch whose records were all fsynced by the callback."""
        committed = self._consumer.commit(message=message, asynchronous=False)
        if not isinstance(committed, (list, tuple)) or len(committed) != 1:
            raise RuntimeError("Kafka offset commit was not confirmed")
        result = committed[0]
        if getattr(result, "error", None) is not None or getattr(result, "err", None) is not None:
            raise RuntimeError("Kafka offset commit returned a partition error")
        if (getattr(result, "topic", None) != self.topic
                or getattr(result, "partition", None) != self.partition
                or getattr(result, "offset", None) != message.offset() + 1):
            raise RuntimeError("Kafka offset commit did not confirm the requested position")
        confirmed = getattr(on_record, "confirmed", None)
        if confirmed is not None:
            confirmed(message.offset())

    def close(self) -> None:
        """Close the consumer."""
        self._consumer.close()
