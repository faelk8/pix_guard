"""End-to-end pipeline checks; these tests run only inside the Spark container."""

import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
import tempfile
from threading import Timer
import uuid

import pytest

pytest.importorskip("pyspark", reason="Spark integration tests require the pipeline image")
from pixguard_ai.application.generate_events import GenerationConfig
from pixguard_ai.domain.pipeline import validate_run_id
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.application.process_events import process_events
from pixguard_ai.infrastructure.spark.runner import SparkRun
from pyspark.sql import SparkSession, functions as F


def _input_file(lines: list[str]) -> Path:
    """Reserve a fixture within the configured data boundary without overwriting data."""
    directory = Path("/data/pixguard/integration-fixtures")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"fixture-{uuid.uuid4().hex}.jsonl"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _query_session() -> SparkSession:
    """Read persisted evidence with a bounded local Spark session."""
    return (SparkSession.builder.master("local[2]").appName("pipeline-regressions")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false").getOrCreate())


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
    manifest = process_events(input_path, root, run_id, SparkRun)
    assert manifest["counts"] == {
        "bronze": 6, "silver_unique": 2, "duplicates_removed": 1,
        "quarantine": 3, "gold": 2,
    }
    assert manifest["reconciliation"]["passed"] is True
    summary = json.loads((root / manifest["dashboard"]["summary_path"]).read_text())
    assert len(summary["top_groups"]) <= 20
    assert any(group["event_day_utc"] == "2024-12-31" for group in summary["top_groups"])
    snapshot = Path(manifest["input"]["snapshot_path"])
    assert snapshot.read_bytes() == input_path.read_bytes()
    assert manifest["input"]["sha256"] == sha256(snapshot.read_bytes()).hexdigest()
    assert manifest["input"]["snapshot_bytes"] == snapshot.stat().st_size


def test_invalid_member_of_conflicting_group_is_quarantined_once() -> None:
    """Overlapping invalid/conflict rules preserve exactly one disposition per input."""
    root = Path("/data/pixguard").resolve()
    events = list(SyntheticEventSource().events(GenerationConfig(seed=497, count=2)))
    first = events[0].to_dict()
    conflict, invalid = deepcopy(first), deepcopy(first)
    conflict["features"]["amount_brl"] = "777.77"
    invalid["features"]["amount_brl"] = "bad"
    invalid_json = json.dumps(invalid, sort_keys=True)
    lines = [json.dumps(first, sort_keys=True), json.dumps(conflict, sort_keys=True),
             invalid_json, json.dumps(events[1].to_dict(), sort_keys=True)]
    input_path = _input_file(lines)
    manifest = process_events(input_path, root, f"conflict-{uuid.uuid4().hex}", SparkRun)
    assert manifest["counts"] == {"bronze": 4, "silver_unique": 1, "duplicates_removed": 0,
                                  "quarantine": 3, "gold": 1}
    assert manifest["reconciliation"]["passed"] is True
    spark = _query_session()
    try:
        quarantine = spark.read.parquet(str(root / manifest["outputs"]["quarantine"]))
        assert quarantine.filter(F.col("raw_json") == invalid_json).count() == 1
        reasons = {row["quarantine_reason"]: row["count"]
                   for row in quarantine.groupBy("quarantine_reason").count().collect()}
        assert reasons == {"invalid_payment_event_v1": 1, "conflicting_event_id_payload": 2}
    finally:
        spark.stop()


@pytest.mark.parametrize("replacement_stage", ["during_run", "after_run"])
def test_source_replacement_cannot_publish_mismatched_completed_manifest(
        monkeypatch: pytest.MonkeyPatch, replacement_stage: str) -> None:
    """Every view reads the snapshot, and replacing the original prevents publication."""
    root = Path("/data/pixguard").resolve()
    events = list(SyntheticEventSource().events(GenerationConfig(seed=498, count=2)))
    original_json = json.dumps(events[0].to_dict(), sort_keys=True)
    replacement_bytes = (json.dumps(events[1].to_dict(), sort_keys=True) + "\n").encode()
    input_path = _input_file([original_json])
    original_bytes = input_path.read_bytes()
    run = SparkRun(input_path, root, f"replace-{uuid.uuid4().hex}")
    invoke = run._invoke
    replaced: list[bool] = []

    def replace_source() -> None:
        candidate = input_path.with_suffix(".replacement")
        candidate.write_bytes(replacement_bytes)
        candidate.replace(input_path)
        replaced.append(True)

    def invoke_with_replacement(mode: str, spec: Path) -> None:
        if mode == "run" and replacement_stage == "during_run":
            timer = Timer(0.5, replace_source)
            timer.start()
            try:
                invoke(mode, spec)
            finally:
                timer.join()
        else:
            invoke(mode, spec)
            if mode == "run":
                replace_source()

    monkeypatch.setattr(run, "_invoke", invoke_with_replacement)
    with pytest.raises(RuntimeError, match="input changed during processing"):
        run.execute()
    assert replaced == [True]
    assert json.loads((run.run_dir / "manifest.json").read_text())["status"] == "failed"
    snapshot = run.run_dir / "input-snapshot.jsonl"
    assert snapshot.read_bytes() == original_bytes
    assert input_path.read_bytes() == replacement_bytes
    assert not run.lock_path.exists()
    tables = run._table_paths(run.run_dir / "spark-warehouse")
    spark = _query_session()
    try:
        bronze = spark.read.parquet(str(tables["bronze"]))
        assert [row.raw_json for row in bronze.select("raw_json").collect()] == [original_json]
        assert bronze.select("source_file").first().source_file == str(input_path)
        assert spark.read.parquet(str(tables["silver"])).count() == 1
        assert spark.read.parquet(str(tables["quarantine"])).count() == 0
        assert spark.read.parquet(str(tables["gold"])).agg(F.sum("event_count")).first()[0] == 1
    finally:
        spark.stop()


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
    fixture = _input_file(["{}"])
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
