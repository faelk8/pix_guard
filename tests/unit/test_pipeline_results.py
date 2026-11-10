"""Validate bounded Spark run results before exposing them in Streamlit."""

import json
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from pixguard_ai.infrastructure.local import pipeline_results as results
from pixguard_ai.interfaces import dashboard


def make_run(root: Path, run_id: str, finished_at: str = "2026-11-09T12:00:00+00:00") -> dict[str, object]:
    """Write one small, complete run fixture with real paths and a Parquet marker."""
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True)
    input_path = root / "events.jsonl"
    input_path.write_text("{}\n", encoding="utf-8")
    layers = {name: run_dir / "warehouse" / name for name in
              ("bronze", "classified", "conflicting_ids", "quarantine", "silver", "gold")}
    for path in layers.values():
        path.mkdir(parents=True)
    (layers["gold"] / "part-00000.parquet").write_bytes(b"parquet")
    group = {"event_day_utc": "2026-11-09", "merchant_id": "merchant-1",
             "channel": "app", "status": "approved", "event_count": 1,
             "amount_total_brl": "10.25"}
    summary = {"label_is_synthetic": True, "top_groups": [group], "gold_group_count": 1}
    summary_path = run_dir / "dashboard-summary.json"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    manifest = {
        "manifest_version": 1, "run_id": run_id, "status": "completed",
        "schema_version": "payment_event_v1", "pipeline_version": "1.0.0",
        "label_is_synthetic": True,
        "input": {"path": str(input_path), "sha256": "a" * 64, "rows": 1},
        "counts": {"bronze": 1, "silver_unique": 1, "duplicates_removed": 0,
                   "quarantine": 0, "gold": 1},
        "reconciliation": {"formula": "input = unique_valid + duplicates + quarantine", "passed": True},
        "outputs": {name: str(path.relative_to(root)) for name, path in layers.items()},
        "dashboard": {"gold_path": str(layers["gold"].relative_to(root)),
                      "summary_path": str(summary_path.relative_to(root)),
                      "summary": {"gold_rows": 1, "top_groups": [group]}},
        "started_at": (datetime.fromisoformat(finished_at) - timedelta(minutes=1)).isoformat(),
        "finished_at": finished_at,
        "elapsed_seconds": 1.0,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest


def test_selects_newest_completed_run_and_uses_bounded_summary(tmp_path: Path) -> None:
    """The newest valid completed manifest supplies counts and summary rows."""
    make_run(tmp_path, "run-old", "2026-11-08T12:00:00+00:00")
    manifest = make_run(tmp_path, "run-new")
    snapshot = results.load_latest_pipeline_results(tmp_path)
    assert snapshot.manifest == manifest
    assert snapshot.top_groups[0]["amount_total_brl"] == "10.25"
    assert snapshot.manifest["run_id"] == "run-new"
    assert not snapshot.error


def test_failed_run_does_not_replace_latest_completed(tmp_path: Path) -> None:
    """A later failed attempt warns while keeping the last completed snapshot."""
    manifest = make_run(tmp_path, "run-good")
    failed = tmp_path / "runs" / "run-failed"
    failed.mkdir()
    (failed / "manifest.json").write_text(json.dumps({
        "manifest_version": 1, "run_id": "run-failed", "status": "failed", "error_type": "RuntimeError",
    }), encoding="utf-8")
    snapshot = results.load_latest_pipeline_results(tmp_path)
    assert snapshot.manifest == manifest
    assert snapshot.top_groups
    assert snapshot.newer_failure == "run-failed"


@pytest.mark.parametrize("mutation", ["counts", "utc", "summary", "path"])
def test_inconsistent_completed_run_has_no_metrics(tmp_path: Path, mutation: str) -> None:
    """Reject mismatched reconciliation, non-UTC dates, oversized summaries, and escapes."""
    manifest = make_run(tmp_path, "run-bad")
    run_dir = tmp_path / "runs" / "run-bad"
    if mutation == "counts":
        manifest["counts"]["quarantine"] = 1
    elif mutation == "utc":
        manifest["finished_at"] = "2026-11-09T15:00:00+03:00"
    elif mutation == "summary":
        manifest["dashboard"]["summary"]["top_groups"] = [{}] * 21
    else:
        manifest["dashboard"]["summary_path"] = "../outside.json"
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    snapshot = results.load_latest_pipeline_results(tmp_path)
    assert snapshot.manifest is None and snapshot.error
    assert not snapshot.top_groups


def test_rejects_oversized_and_duplicate_key_manifest(tmp_path: Path) -> None:
    """Manifest reads stay bounded and duplicate keys cannot shadow trusted values."""
    first = tmp_path / "runs" / "run-large"
    first.mkdir(parents=True)
    (first / "manifest.json").write_bytes(b" " * (results.MAX_MANIFEST_BYTES + 1))
    snapshot = results.load_latest_pipeline_results(tmp_path)
    assert snapshot.error and snapshot.manifest is None

    make_run(tmp_path, "run-summary-large")
    (tmp_path / "runs" / "run-summary-large" / "dashboard-summary.json").write_bytes(
        b" " * (results.MAX_SUMMARY_BYTES + 1))
    snapshot = results.load_latest_pipeline_results(tmp_path)
    assert snapshot.error and snapshot.manifest is None

    (first / "manifest.json").write_text('{"status":"completed","status":"failed"}', encoding="utf-8")
    snapshot = results.load_latest_pipeline_results(tmp_path)
    assert snapshot.error and snapshot.manifest is None


def test_no_runs_is_an_explicit_empty_state(tmp_path: Path) -> None:
    """An empty shared volume is distinct from a failed or invalid result."""
    snapshot = results.load_latest_pipeline_results(tmp_path)
    assert snapshot.manifest is None and not snapshot.error
    assert "Nenhuma" in snapshot.message


def test_dashboard_renders_counts_and_exact_decimal_without_money_chart() -> None:
    """The FASE 3 view uses integer counts for its chart and decimal text for BRL."""
    manifest = {"run_id": "run-ui", "started_at": "2026-11-09T11:59:00+00:00",
                "finished_at": "2026-11-09T12:00:00+00:00",
                "counts": {"bronze": 100_000, "silver_unique": 99_000,
                           "duplicates_removed": 500, "quarantine": 500, "gold": 1}}
    rows = ({"event_day_utc": "2026-11-09", "merchant_id": "merchant-1", "channel": "app",
             "status": "approved", "event_count": 99_000, "amount_total_brl": "123456789.01"},)

    class Column:
        def metric(self, label: str, value: str) -> None:
            captured["metrics"].append((label, value))

    class RecordingStreamlit:
        def __getattr__(self, name: str) -> Callable[..., None]:
            def record(*args: object, **kwargs: object) -> None:
                captured[name].append((args, kwargs))
            return record

        def columns(self, count: int) -> list[Column]:
            return [Column() for _ in range(count)]

    captured: dict[str, list[object]] = {key: [] for key in
                                         ("metrics", "caption", "success", "warning", "write",
                                          "subheader", "dataframe", "bar_chart")}
    dashboard.render_pipeline_results(RecordingStreamlit(), results.PipelineSnapshot(
        manifest, rows, "Results OK"))
    assert ("Entrada / Bronze", "100.000") in captured["metrics"]
    table = captured["dataframe"][0][0][0]
    assert table[0]["Total BRL"] == "123456789.01"
    chart = captured["bar_chart"][0][0][0]
    assert chart == {"Eventos por grupo (top 20)": [99_000]}
    assert any("não medem desempenho antifraude real" in str(call) for call in captured["warning"])
