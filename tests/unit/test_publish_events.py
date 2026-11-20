"""Delivery accounting and snapshot protection independent of a broker."""

import json
from pathlib import Path

import pytest

from pixguard_ai.application.generate_events import GenerationConfig, generate_events
from pixguard_ai.application.publish_events import publish_events
from pixguard_ai.infrastructure.kafka.storage import FilePublicationStore, JsonlPublicationSource
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.infrastructure.synthetic.jsonl import JsonlEventWriter


class FakePublisher:
    def __init__(self, fail_at=None, missing_ack=False):
        self.sent = []
        self.fail_at = fail_at
        self.missing_ack = missing_ack

    def inspect(self, timeout):
        return {"cluster_id": "test", "topic": "test"}

    def send(self, key, payload, headers, callback):
        if len(self.sent) == self.fail_at:
            raise OSError("simulated send failure")
        self.sent.append((key, payload, headers))
        if not self.missing_ack:
            callback({"cluster_id": "test", "topic": "test", "partition": 0,
                      "offset": len(self.sent)-1})

    def flush(self, timeout):
        return int(self.missing_ack)


def setup(tmp_path, count=10):
    path = tmp_path / "events.jsonl"
    generate_events(GenerationConfig(seed=42, count=count), SyntheticEventSource(), JsonlEventWriter(path))
    store = FilePublicationStore(tmp_path / "publications")
    return path, store, JsonlPublicationSource(path, store)


def manifest(store):
    return json.loads((store.directory / "manifest.json").read_text())


def test_complete_and_reexecution(tmp_path):
    path, store, source = setup(tmp_path)
    original = path.read_bytes().splitlines()
    publisher = FakePublisher()
    result = publish_events("batch", source, publisher, store, window_size=3)
    assert (result["confirmed"], result["uncertain"], result["not_sent"]) == (10, 0, 0)
    assert [entry[1] for entry in publisher.sent] == original
    assert [entry[2]["source_ordinal"] for entry in publisher.sent] == list(map(str, range(10)))
    before = (store.directory / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        publish_events("batch", source, publisher, store)
    assert (store.directory / "manifest.json").read_bytes() == before


@pytest.mark.parametrize("kind", ["checksum", "count", "incomplete", "contract", "duplicate_field"])
def test_bad_source_never_sends(tmp_path, kind):
    path, store, source = setup(tmp_path)
    sidecar = path.with_suffix(".manifest.json")
    data = json.loads(sidecar.read_text())
    if kind == "checksum":
        data["sha256"] = "0" * 64
    elif kind == "count":
        data["count_written"] += 1
        data["count_requested"] += 1
    elif kind == "incomplete":
        data["status"] = "running"
    elif kind == "contract":
        path.write_bytes(path.read_bytes().replace(b'payment_event_v1', b'payment_event_v2'))
    else:
        path.write_bytes(path.read_bytes().replace(b'{"event_id":', b'{"event_id":"bad","event_id":', 1))
    sidecar.write_text(json.dumps(data))
    publisher = FakePublisher()
    with pytest.raises(ValueError):
        publish_events("batch", source, publisher, store)
    assert not publisher.sent
    assert manifest(store)["status"] == "failed"


def test_mutable_original_is_not_reopened(tmp_path):
    path, store, source = setup(tmp_path)
    original = path.read_bytes().splitlines()
    publisher = FakePublisher()
    def inspect(timeout):
        path.write_text("mutated\n")
        return {"topic": "test"}
    publisher.inspect = inspect
    publish_events("batch", source, publisher, store)
    assert [entry[1] for entry in publisher.sent] == original


@pytest.mark.parametrize("missing_ack,fail_at,expected", [(True, None, (0, 3, 7)), (False, 1, (1, 1, 8))])
def test_failure_has_no_false_success(tmp_path, missing_ack, fail_at, expected):
    _, store, source = setup(tmp_path)
    with pytest.raises((TimeoutError, OSError)):
        publish_events("batch", source, FakePublisher(fail_at, missing_ack), store, window_size=3)
    result = manifest(store)
    assert result["status"] == "failed"
    assert (result["confirmed"], result["uncertain"], result["not_sent"]) == expected


def test_crash_reservation_precedes_send(tmp_path):
    _, store, source = setup(tmp_path)
    publisher = FakePublisher()
    def send(*args):
        observed = manifest(store)
        assert observed["status"] == "publishing"
        assert (observed["confirmed"], observed["uncertain"], observed["not_sent"]) == (0, 3, 7)
        raise KeyboardInterrupt
    publisher.send = send
    with pytest.raises(KeyboardInterrupt):
        publish_events("batch", source, publisher, store, window_size=3)
    assert manifest(store)["status"] == "interrupted"


def test_disk_failure_preserves_uncertain_checkpoint(tmp_path):
    _, store, source = setup(tmp_path)
    def fail(window, records):
        raise OSError("disk full")
    store.receipts = fail
    with pytest.raises(OSError):
        publish_events("batch", source, FakePublisher(), store, window_size=3)
    result = manifest(store)
    assert result["status"] == "failed"
    assert (result["confirmed"], result["uncertain"]) == (0, 3)


@pytest.mark.parametrize("batch", ["../bad", "/bad", "", "a/b", "a"*65])
def test_batch_path_rejected(tmp_path, batch):
    _, store, source = setup(tmp_path)
    with pytest.raises(ValueError):
        publish_events(batch, source, FakePublisher(), store)


@pytest.mark.parametrize("contents", ["[]", "null", " " * 65537])
def test_manifest_shape_and_size_rejected(tmp_path, contents):
    path, store, source = setup(tmp_path)
    path.with_suffix(".manifest.json").write_text(contents)
    publisher = FakePublisher()
    with pytest.raises(ValueError):
        publish_events("batch", source, publisher, store)
    assert not publisher.sent


def test_changed_snapshot_never_completes(tmp_path):
    _, store, source = setup(tmp_path)
    publisher = FakePublisher()
    original = publisher.flush
    def flush(timeout):
        source.snapshot.chmod(0o600)
        with source.snapshot.open("ab") as stream:
            stream.write(b"\n")
        return original(timeout)
    publisher.flush = flush
    with pytest.raises(ValueError):
        publish_events("batch", source, publisher, store)
    result = manifest(store)
    assert result["status"] == "failed" and result["confirmed"] == 10


def test_non_utf8_json_is_rejected(tmp_path):
    path, store, source = setup(tmp_path)
    path.write_bytes(path.read_text().encode("utf-16-be"))
    publisher = FakePublisher()
    with pytest.raises(ValueError):
        publish_events("batch", source, publisher, store)
    assert not publisher.sent


def test_new_multilevel_namespace_is_durable_before_send(tmp_path, monkeypatch):
    from pixguard_ai.infrastructure.kafka import storage

    path, _, _ = setup(tmp_path)
    root = tmp_path / "new" / "nested" / "publications"
    store = FilePublicationStore(root)
    source = JsonlPublicationSource(path, store)
    synced = []
    actual_sync = storage.sync_directory

    def observe(directory):
        synced.append(directory)
        actual_sync(directory)

    monkeypatch.setattr(storage, "sync_directory", observe)
    publisher = FakePublisher()
    original_send = publisher.send

    def send(*args):
        # Parent entries are durable in top-down order before the first offer.
        expected = [tmp_path, tmp_path / "new", tmp_path / "new" / "nested", root]
        index = synced.index(tmp_path)
        assert synced[index:index + len(expected)] == expected
        assert store.directory in synced  # initial manifest directory fsync
        original_send(*args)

    publisher.send = send
    publish_events("batch", source, publisher, store)
    assert len(publisher.sent) == 10


def test_concurrently_created_namespace_still_syncs_parents(tmp_path, monkeypatch):
    from pixguard_ai.infrastructure.kafka import storage

    root = tmp_path / "new" / "nested"
    store = FilePublicationStore(root)
    actual_mkdir = Path.mkdir
    synced = []

    def competing_mkdir(path, *args, **kwargs):
        if path == tmp_path / "new" and not path.exists():
            # Simulate another writer winning mkdir without having fsynced yet.
            actual_mkdir(path)
        return actual_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", competing_mkdir)
    monkeypatch.setattr(storage, "sync_directory", synced.append)
    store.reserve("batch")
    assert synced[-3:] == [tmp_path, tmp_path / "new", root]
    assert store.directory == root / "batch"


def test_namespace_sync_failure_prevents_publication(tmp_path, monkeypatch):
    from pixguard_ai.infrastructure.kafka import storage

    path, _, _ = setup(tmp_path)
    store = FilePublicationStore(tmp_path / "new" / "nested")
    publisher = FakePublisher()

    def fail(directory):
        if directory == tmp_path:
            raise OSError("namespace fsync failed")

    monkeypatch.setattr(storage, "sync_directory", fail)
    with pytest.raises(OSError):
        publish_events("batch", JsonlPublicationSource(path, store), publisher, store)
    assert not publisher.sent
    assert store.directory is None


def test_existing_unsynced_ancestor_is_not_assumed_durable(tmp_path, monkeypatch):
    from pixguard_ai.infrastructure.kafka import storage

    # A competing process has made A visible, but has not synced A.parent yet.
    ancestor = tmp_path / "visible-but-unsynced"
    ancestor.mkdir()
    root = ancestor / "publications"
    synced = []
    monkeypatch.setattr(storage, "sync_directory", synced.append)
    store = FilePublicationStore(root)
    store.reserve("batch")
    assert synced[-3:] == [tmp_path, ancestor, root]
    assert synced.index(tmp_path) < synced.index(ancestor)


def test_namespace_alias_uses_canonical_ancestor_chain(tmp_path, monkeypatch):
    from pixguard_ai.infrastructure.kafka import storage

    physical = tmp_path / "physical"
    physical.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(physical, target_is_directory=True)
    synced = []
    monkeypatch.setattr(storage, "sync_directory", synced.append)
    store = FilePublicationStore(alias / "nested" / "publications")
    store.reserve("batch")
    assert store.root == physical / "nested" / "publications"
    assert store.directory == store.root / "batch"
    assert synced[-4:] == [tmp_path, physical, physical / "nested", store.root]
    assert alias not in synced
