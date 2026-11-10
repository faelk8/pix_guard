"""End-to-end pipeline checks; these tests run only inside the Spark container."""

import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
import tempfile
import uuid

import pytest

pytest.importorskip("pyspark", reason="Spark integration tests require the pipeline image")
from pixguard_ai.application.generate_events import GenerationConfig
from pixguard_ai.domain.pipeline import validate_run_id
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.application.process_events import process_events
from pixguard_ai.infrastructure.spark.runner import SparkRun


def test_invalid_duplicate_and_conflicting_records_reconcile() -> None:
    """The SDP pipeline deduplicates identical payloads and quarantines conflicts."""
    root = Path("/data/pixguard").resolve()
    fixture_dir = root / "integration-fixtures"
    fixture_dir.mkdir(parents=True, exist_ok=True)
    events = list(SyntheticEventSource().events(GenerationConfig(seed=991, count=3)))
    duplicate = json.dumps(events[0].to_dict(), sort_keys=True, separators=(",", ":"))
    duplicate_spaced = json.dumps(events[0].to_dict(), separators=(", ", ": "))
    conflicted = events[1].to_dict()
    conflicted["features"]["amount_brl"] = "999.99"
    late_event = replace(events[2], event_time=events[2].event_time - timedelta(days=1))
    lines = [duplicate, duplicate_spaced,
             json.dumps(events[1].to_dict(), sort_keys=True, separators=(",", ":")),
             json.dumps(conflicted, sort_keys=True, separators=(",", ":")),
             json.dumps(late_event.to_dict(), sort_keys=True, separators=(",", ":")),
             "{invalid-json"]
    input_path = fixture_dir / f"fixture-{uuid.uuid4().hex}.jsonl"
    input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    run_id = validate_run_id(f"test-{uuid.uuid4().hex}")
    manifest = process_events(input_path, root, run_id)
    assert manifest["counts"] == {
        "bronze": 6, "silver_unique": 2, "duplicates_removed": 1,
        "quarantine": 3, "gold": 2,
    }
    assert manifest["reconciliation"]["passed"] is True
    summary = json.loads((root / manifest["dashboard"]["summary_path"]).read_text())
    assert len(summary["top_groups"]) <= 20
    assert any(group["event_day_utc"] == "2024-12-31" for group in summary["top_groups"])


def test_path_validation_rejects_run_traversal() -> None:
    """Run identifiers cannot escape the shared volume root."""
    try:
        validate_run_id("../outside")
    except ValueError:
        return
    raise AssertionError("traversal token should be rejected")


def test_existing_run_lock_blocks_execution() -> None:
    """A competing execution is rejected without removing its lock."""
    root = Path("/data/pixguard").resolve()
    fixture = root / "events.jsonl"
    run_id = validate_run_id(f"locked-{uuid.uuid4().hex}")
    run = SparkRun(fixture, root, run_id)
    run.lock_path.parent.mkdir(parents=True, exist_ok=True)
    run.lock_path.write_text("already-held", encoding="utf-8")
    try:
        with pytest.raises(RuntimeError, match="locked"):
            run.execute()
        assert run.lock_path.read_text(encoding="utf-8") == "already-held"
    finally:
        run.lock_path.unlink(missing_ok=True)


def test_input_path_must_remain_under_data_root() -> None:
    """Inputs outside the shared data root are rejected before execution."""
    root = Path("/data/pixguard").resolve()
    with tempfile.NamedTemporaryFile() as external:
        with pytest.raises(ValueError, match="inside /data/pixguard"):
            SparkRun(Path(external.name), root, f"outside-{uuid.uuid4().hex}")
