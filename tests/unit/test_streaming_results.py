"""Validate bounded Kafka manifest snapshots before dashboard rendering."""

import json
from pathlib import Path

import pytest

from pixguard_ai.infrastructure.local.streaming_results import (
    StreamingSnapshot,
    load_latest_streaming_results,
)


def write_json(path: Path, value: object) -> None:
    """Write an isolated manifest fixture."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def publication(status: str = "completed") -> dict[str, object]:
    """Return a reconciled publication manifest fixture."""
    total = 10
    return {
        "manifest_version": 1,
        "source_batch_id": "batch-1",
        "status": status,
        "confirmed": total if status == "completed" else 7,
        "uncertain": 0 if status == "completed" else 1,
        "not_sent": 0 if status == "completed" else 2,
        "total": total,
        "source_sha256": "a" * 64,
        "started_at": "2026-11-20T10:00:00+00:00",
        "finished_at": "2026-11-20T10:00:10+00:00",
        "error": None,
    }


def capture(status: str = "completed") -> dict[str, object]:
    """Return a bounded capture manifest fixture."""
    value: dict[str, object] = {
        "capture_manifest_version": 1,
        "capture_id": "capture-1",
        "status": status,
        "cluster_id": "cluster-1",
        "topic": "pixguard.payment_event_v1",
        "partition": 0,
        "start": 0,
        "end": 10,
        "count": 10,
        "started_at": "2026-11-20T10:00:11+00:00",
        "finished_at": "2026-11-20T10:00:21+00:00",
    }
    if status == "completed":
        value["segment"] = {"sha256": "b" * 64, "size_bytes": 100}
    return value


def snapshot() -> dict[str, object]:
    """Return a reconciled snapshot manifest fixture."""
    counts = {
        "physical": 10,
        "exported": 8,
        "duplicates": 1,
        "transport_quarantine": 1,
        "contract_quarantine": 0,
    }
    return {
        "snapshot_manifest_version": 1,
        "snapshot_id": "snapshot-1",
        "capture_id": "capture-1",
        "status": "completed",
        "capture": {"cluster_id": "cluster-1", "topic": "pixguard.payment_event_v1",
                    "partition": 0, "start": 0, "end": 10,
                    "segment_sha256": "b" * 64},
        "counts": counts,
        "reconciliation": {"passed": True},
        "finished_at": "2026-11-20T10:00:22+00:00",
        "events_sha256": "c" * 64,
        "quarantine_sha256": "d" * 64,
    }


def test_loads_all_stages_and_separates_counts(tmp_path: Path) -> None:
    write_json(tmp_path / "publications" / "batch-1" / "manifest.json", publication())
    write_json(tmp_path / "captures" / "capture-1" / "manifest.json", capture())
    write_json(tmp_path / "captures" / "capture-1" / "snapshots" / "snapshot-1" / "manifest.json",
               snapshot())

    result = load_latest_streaming_results(tmp_path)

    assert result.publication["confirmed"] == 10
    assert result.capture["count"] == 10
    assert result.snapshot["counts"]["transport_quarantine"] == 1
    assert not result.error


@pytest.mark.parametrize("kind", ["publication", "capture", "snapshot"])
def test_partial_or_invalid_stage_is_not_promoted_to_success(tmp_path: Path, kind: str) -> None:
    if kind == "publication":
        value = publication()
        value["confirmed"] = 9
        write_json(tmp_path / "publications" / "batch-1" / "manifest.json", value)
    elif kind == "capture":
        value = capture()
        value["status"] = "capturing"
        write_json(tmp_path / "captures" / "capture-1" / "manifest.json", value)
    else:
        value = snapshot()
        value["counts"]["physical"] = 11
        write_json(tmp_path / "captures" / "capture-1" / "snapshots" / "snapshot-1" / "manifest.json",
                   value)

    result = load_latest_streaming_results(tmp_path)
    if kind == "publication":
        assert result.publication is None
        assert result.error
    elif kind == "snapshot":
        assert result.snapshot is None
        assert result.error
    else:
        assert result.capture["status"] == "capturing"


def test_empty_and_duplicate_key_manifests_are_explicit(tmp_path: Path) -> None:
    empty = load_latest_streaming_results(tmp_path)
    assert empty.publication is None and not empty.error

    write_json(tmp_path / "publications" / "batch-1" / "manifest.json", publication())
    path = tmp_path / "publications" / "batch-1" / "manifest.json"
    path.write_text(
        '{"manifest_version":1,"manifest_version":2,"status":"completed"}',
        encoding="utf-8",
    )
    invalid = load_latest_streaming_results(tmp_path)
    assert invalid.publication is None and invalid.error


def test_malformed_status_and_symlink_capture_are_fail_closed(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside-capture"
    outside.mkdir(exist_ok=True)
    write_json(outside / "manifest.json", capture())
    (tmp_path / "captures").mkdir()
    (tmp_path / "captures" / "capture-link").symlink_to(outside, target_is_directory=True)
    malformed = load_latest_streaming_results(tmp_path)
    assert malformed.error

    write_json(tmp_path / "publications" / "batch-1" / "manifest.json",
               {**publication(), "status": []})
    invalid = load_latest_streaming_results(tmp_path)
    assert invalid.publication is None and invalid.error


def test_snapshot_must_match_presented_capture(tmp_path: Path) -> None:
    write_json(tmp_path / "captures" / "capture-1" / "manifest.json", capture())
    mismatched = snapshot()
    mismatched["capture_id"] = "capture-other"
    mismatched["capture"]["cluster_id"] = "cluster-other"
    write_json(
        tmp_path / "captures" / "capture-1" / "snapshots" / "snapshot-1" / "manifest.json",
        mismatched,
    )
    result = load_latest_streaming_results(tmp_path)
    assert result.snapshot is None
    assert result.error


@pytest.mark.parametrize("status", ["capturing", "completed"])
def test_latest_capture_does_not_replace_previous_completed_snapshot(
        tmp_path: Path, status: str) -> None:
    """A new capture has its own lifecycle, independent of the last export."""
    write_json(tmp_path / "captures" / "capture-1" / "manifest.json", capture())
    write_json(tmp_path / "captures" / "capture-1" / "snapshots" / "snapshot-1" / "manifest.json",
               snapshot())
    newer = capture(status)
    newer.update(capture_id="capture-2", started_at="2026-11-20T11:00:00+00:00",
                 finished_at="2026-11-20T11:00:10+00:00")
    write_json(tmp_path / "captures" / "capture-2" / "manifest.json", newer)
    result = load_latest_streaming_results(tmp_path)
    assert not result.error
    assert result.capture["capture_id"] == "capture-2"
    assert result.snapshot["capture_id"] == "capture-1"


@pytest.mark.parametrize("offset", [-2, True, "5", 4, 10, 11])
def test_capture_rejects_confirmed_offsets_outside_its_range(tmp_path: Path, offset: object) -> None:
    value = capture("capturing")
    value.update(start=5, last_confirmed_offset=offset)
    write_json(tmp_path / "captures" / "capture-1" / "manifest.json", value)
    result = load_latest_streaming_results(tmp_path)
    assert result.capture is None and result.error


def test_valid_confirmed_offset_does_not_imply_record_count(tmp_path: Path) -> None:
    value = capture("capturing")
    value.update(start=5, last_confirmed_offset=8)
    del value["count"]
    del value["finished_at"]
    write_json(tmp_path / "captures" / "capture-1" / "manifest.json", value)
    result = load_latest_streaming_results(tmp_path)
    assert not result.error
    assert result.capture["last_confirmed_offset"] == 8
    assert "count" not in result.capture


@pytest.mark.parametrize("kind", ["publication", "capture"])
@pytest.mark.parametrize("finish", ["missing", None, "invalid", "2026-11-20T09:00:00+00:00",
                                    "2026-11-20T10:00:22-03:00"])
def test_terminal_stage_requires_coherent_utc_finish(
        tmp_path: Path, kind: str, finish: object) -> None:
    value = publication() if kind == "publication" else capture()
    value.pop("finished_at")
    if finish != "missing":
        value["finished_at"] = finish
    directory = "publications/batch-1" if kind == "publication" else "captures/capture-1"
    write_json(tmp_path / directory / "manifest.json", value)
    assert load_latest_streaming_results(tmp_path).error


@pytest.mark.parametrize("kind", ["publication", "capture"])
def test_active_stage_can_omit_finish(tmp_path: Path, kind: str) -> None:
    value = publication("publishing") if kind == "publication" else capture("capturing")
    del value["finished_at"]
    directory = "publications/batch-1" if kind == "publication" else "captures/capture-1"
    write_json(tmp_path / directory / "manifest.json", value)
    assert not load_latest_streaming_results(tmp_path).error


@pytest.mark.parametrize("unsafe", ["invalid", "symlink", "namespace_symlink"])
def test_unsafe_snapshot_cannot_be_hidden_by_a_valid_candidate(tmp_path: Path, unsafe: str) -> None:
    write_json(tmp_path / "captures" / "capture-1" / "manifest.json", capture())
    directory = tmp_path / "captures" / "capture-1" / "snapshots"
    write_json(directory / "snapshot-1" / "manifest.json", snapshot())
    if unsafe == "invalid":
        value = snapshot()
        value["counts"]["physical"] = 999
        write_json(directory / "snapshot-bad" / "manifest.json", value)
    elif unsafe == "symlink":
        (directory / "snapshot-link").symlink_to(directory / "snapshot-1", target_is_directory=True)
    else:
        write_json(tmp_path / "captures" / "capture-2" / "manifest.json",
                   {**capture(), "capture_id": "capture-2"})
        (tmp_path / "captures" / "capture-2" / "snapshots").symlink_to(directory,
                                                                 target_is_directory=True)
    result = load_latest_streaming_results(tmp_path)
    assert result.snapshot is None and result.error


@pytest.mark.parametrize("field", ["cluster_id", "topic", "partition", "start", "end",
                                   "segment_sha256"])
def test_snapshot_provenance_matches_its_parent(tmp_path: Path, field: str) -> None:
    write_json(tmp_path / "captures" / "capture-1" / "manifest.json", capture())
    value = snapshot()
    value["capture"][field] = "mismatch"
    write_json(tmp_path / "captures" / "capture-1" / "snapshots" / "snapshot-1" / "manifest.json",
               value)
    assert load_latest_streaming_results(tmp_path).error



def test_snapshot_parent_manifest_symlink_is_rejected_before_reading(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Do not follow an external parent manifest while validating a snapshot."""
    from pixguard_ai.infrastructure.local import streaming_results

    root = tmp_path / "data"
    external = tmp_path / "external" / "manifest.json"
    write_json(external, capture())
    directory = root / "captures" / "capture-1"
    write_json(directory / "snapshots" / "snapshot-1" / "manifest.json", snapshot())
    (directory / "manifest.json").symlink_to(external)
    original_reader = streaming_results._read_json
    read_paths: list[Path] = []

    def guarded_reader(path: Path) -> dict[str, object]:
        assert path.resolve().is_relative_to(root.resolve()), "external manifest read attempted"
        read_paths.append(path)
        return original_reader(path)

    monkeypatch.setattr(streaming_results, "_read_json", guarded_reader)
    result = load_latest_streaming_results(root)
    assert result.error and result.capture is None and result.snapshot is None
    assert directory / "manifest.json" not in read_paths
