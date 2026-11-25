"""Real Kafka capture and replay acceptance for STREAM-007."""

import os
from collections.abc import Iterator
from time import monotonic, sleep
from uuid import uuid4

from confluent_kafka.admin import AdminClient, NewTopic
import pytest


@pytest.fixture
def topic() -> Iterator[tuple[str, str, AdminClient]]:
    """Create and clean one isolated real Kafka topic."""
    bootstrap = os.environ.get("PIXGUARD_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    name = "stream007-" + uuid4().hex
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




def pytest_collection_modifyitems(config, items) -> None:
    """Keep destructive acceptance fixtures opt-in to the isolated runner."""
    if os.environ.get("PIXGUARD_STREAM007_ACCEPTANCE") == "1":
        return
    directory = __import__("pathlib").Path(__file__).parent
    for item in items:
        if directory in item.path.parents:
            item.add_marker(pytest.mark.skip(reason="set PIXGUARD_STREAM007_ACCEPTANCE=1 in isolated Compose"))
