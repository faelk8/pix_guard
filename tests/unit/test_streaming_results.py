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
