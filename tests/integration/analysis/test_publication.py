"""End-to-end publication failures and persisted prediction isolation."""

from pathlib import Path

import pytest
pytest.importorskip("pyspark", reason="Analysis integration tests require the pipeline image")
from pyspark.sql import functions as F

from pixguard_ai.domain.analysis import RuleConfig
from pixguard_ai.infrastructure.local.analysis_artifacts import AnalysisArtifacts, atomic_json, read_json
from pixguard_ai.infrastructure.spark.analysis import SparkAnalysis, new_session
from test_analysis import make_source


def test_published_snapshot_and_partial_failure(tmp_path: Path, monkeypatch) -> None:
    spark = new_session("analysis-publication-fixture")
    make_source(spark, tmp_path)
    spark.stop()
    store = AnalysisArtifacts(tmp_path)
    engine = SparkAnalysis(store)
    manifest = engine.evaluate("source", "valid", RuleConfig())
    run, loaded = store.completed("valid")
    assert loaded == manifest
    assert manifest["counts"]["N"] == 2
    assert len(read_json(run / "dashboard-summary.json")["examples"]) == 2
    # A fresh Spark session reads the already durable predictions without labels.
    spark = new_session("analysis-publication-query")
    assert not {"is_suspicious", "ground_truth", "scenario_id"} & set(spark.read.parquet(str(run / "predictions")).columns)
    spark.stop()
    # A failure after predictions are persisted cannot become a completed result.
    def fail_evaluation(*args, **kwargs):
        raise RuntimeError("injected evaluation failure")
    monkeypatch.setattr("pixguard_ai.infrastructure.spark.analysis.evaluate_metrics", fail_evaluation)
    with pytest.raises(RuntimeError, match="injected"):
        engine.evaluate("source", "partial", RuleConfig())
    assert (tmp_path / "analysis-runs/partial/predictions").is_dir()
    assert read_json(tmp_path / "analysis-runs/partial/manifest.json")["status"] == "failed"
    with pytest.raises(ValueError):
        store.completed("partial")
    # A forged count or bounded-summary checksum must not hide publication corruption.
    bad = {**manifest, "counts": {**manifest["counts"], "N": 99}}
    atomic_json(run / "manifest.json", bad)
    with pytest.raises(ValueError, match="reconcile"):
        store.completed("valid")
    atomic_json(run / "manifest.json", manifest)
    (run / "metrics.json").write_text("{}")
    with pytest.raises(ValueError, match="checksum"):
        store.completed("valid")
