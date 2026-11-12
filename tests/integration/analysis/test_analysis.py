"""Real Spark fixtures for monetary parity, ranking, leakage and publication."""

from decimal import Decimal
from pathlib import Path

import pytest
pytest.importorskip("pyspark", reason="Analysis integration tests require the pipeline image")
from pyspark.sql import SparkSession, functions as F, types as T

from pixguard_ai.domain.analysis import RuleConfig, score_amount
from pixguard_ai.infrastructure.local.analysis_artifacts import (
    AnalysisArtifacts, atomic_json, digest, read_json,
)
from pixguard_ai.infrastructure.spark.analysis import (
    SparkAnalysis, evaluate_metrics, join_targets, new_session, score_features,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = new_session("analysis-fixtures")
    yield session
    session.stop()


def frame(spark: SparkSession, rows: list[tuple[str, str, bool]]):
    schema = T.StructType([T.StructField("event_id", T.StringType()),
                           T.StructField("amount_decimal", T.DecimalType(12, 2)),
                           T.StructField("is_suspicious", T.BooleanType())])
    return spark.createDataFrame([(key, Decimal(amount), label) for key, amount, label in rows], schema)


def evaluate(silver, config: RuleConfig = RuleConfig()):
    predictions = score_features(silver.select("event_id", "amount_decimal"), config)
    evaluation = join_targets(predictions, silver.select("event_id", "is_suspicious"))
    return evaluate_metrics(evaluation, silver.count())


def test_rule_parity_and_label_invariance(spark: SparkSession) -> None:
    data = frame(spark, [("a", "9999.99", True), ("b", "10000.00", False), ("c", "10000.01", True)])
    def scored(df):
        return score_features(df.select("event_id", "amount_decimal"), RuleConfig()).orderBy("event_id").collect()
    original = scored(data)
    assert original == scored(data.withColumn("is_suspicious", ~F.col("is_suspicious")))
    assert original == scored(data.drop("is_suspicious"))
    for row, amount in zip(original, ("9999.99", "10000.00", "10000.01"), strict=True):
        assert {key: value for key, value in row.asDict().items() if key != "event_id"} == score_amount(Decimal(amount)).to_dict()
    with pytest.raises(ValueError, match="only"):
        score_features(data, RuleConfig())


def test_known_confusion_and_ranking(spark: SparkSession) -> None:
    data = frame(spark, [("a", "15000.00", True), ("b", "14000.00", True),
                        ("c", "13000.00", False), ("d", "9000.00", True),
                        ("e", "8000.00", False), ("f", "7000.00", False)])
    result = evaluate(data)
    assert [result["counts"][key] for key in ("TP", "FP", "FN", "TN")] == [2, 1, 1, 2]
    for metric in ("precision", "recall", "f1"):
        assert result["metrics"][metric]["value"] == pytest.approx(2 / 3, abs=1e-12)
    assert result["metrics"]["fpr"]["value"] == pytest.approx(1 / 3)
    # Spark trapezoids: 2/3 recall at precision 1, then (2/3+3/4)/2 * 1/3.
    assert result["metrics"]["pr_auc"]["value"] == pytest.approx(65 / 72, abs=1e-12)
    assert result["always_negative"]["counts"] == {"TP": 0, "FP": 0, "FN": 3, "TN": 3}
    assert result["always_negative"]["metrics"]["accuracy"]["value"] == 0.5


@pytest.mark.parametrize("rows,precision,recall,f1,fpr,status", [
    ([], None, None, None, None, "no_evaluable_rows"),
    ([("a", "1.00", False)], None, None, None, 0, "single_class"),
    ([("a", "1.00", True)], None, 0, 0, None, "single_class"),
    ([("a", "10000.00", True)], 1, 1, 1, None, "single_class"),
    ([("a", "10000.00", False)], 0, None, 0, 1, "single_class"),
])
def test_undefined_policy(spark: SparkSession, rows, precision, recall, f1, fpr, status) -> None:
    result = evaluate(frame(spark, rows))
    for key, expected in zip(("precision", "recall", "f1", "fpr"), (precision, recall, f1, fpr), strict=True):
        metric = result["metrics"][key]
        assert metric["value"] == expected
        assert metric["defined"] == (expected is not None)
        assert bool(metric["reason"]) == (expected is None)
    assert result["population_status"] == status
    assert result["metrics"]["pr_auc"]["reason"] == "requires_both_classes"


@pytest.mark.parametrize("rows,expected", [
    ([("a", "2.00", True), ("b", "2.00", False)], 0.5),
    ([("a", "1.00", True), ("b", "2.00", False)], 0.25),
])
def test_ties_and_reversed_ranking(spark: SparkSession, rows, expected) -> None:
    assert evaluate(frame(spark, rows))["metrics"]["pr_auc"]["value"] == pytest.approx(expected, abs=1e-12)


def test_duplicate_and_incomplete_targets(spark: SparkSession) -> None:
    data = frame(spark, [("a", "1.00", True), ("b", "2.00", False)])
    predictions = score_features(data.select("event_id", "amount_decimal"), RuleConfig())
    targets = data.select("event_id", "is_suspicious")
    for broken in (targets.limit(1), targets.union(targets), targets.withColumn("event_id", F.concat(F.lit("x"), "event_id")), targets.withColumn("is_suspicious", F.lit(None).cast("boolean"))):
        with pytest.raises(ValueError):
            join_targets(predictions, broken)


@pytest.mark.parametrize("amount", [None, Decimal("0.00"), Decimal("1000000.01")])
def test_invalid_amount_fails(spark: SparkSession, amount) -> None:
    data = spark.createDataFrame([("a", amount)], "event_id string, amount_decimal decimal(12,2)")
    with pytest.raises(ValueError, match="outside"):
        score_features(data, RuleConfig())


def make_source(spark: SparkSession, root: Path) -> None:
    run = root / "runs/source"
    run.mkdir(parents=True)
    data = frame(spark, [("a", "1.00", False), ("b", "10000.00", True)])
    silver = data.select("event_id", "amount_decimal", F.struct("is_suspicious").alias("ground_truth"), F.lit(True).alias("label_is_synthetic"))
    silver.write.parquet(str(run / "silver"))
    for layer in ("bronze", "quarantine", "gold"):
        (run / layer).mkdir()
    source_input = root / "events.jsonl"
    source_input.write_text("synthetic fixture source\n")
    atomic_json(run / "manifest.json", {
        "manifest_version": 1, "run_id": "source", "status": "completed",
        "schema_version": "payment_event_v1", "pipeline_version": "1.0.0", "label_is_synthetic": True,
        "counts": {"bronze": 2, "silver_unique": 2, "duplicates_removed": 0, "quarantine": 0, "gold": 1},
        "input": {"path": str(source_input), "rows": 2, "sha256": digest(source_input)},
        "reconciliation": {"passed": True},
        "outputs": {name: f"runs/source/{name}" for name in ("silver", "bronze", "quarantine", "gold")},
        "started_at": "2026-11-10T00:00:00+00:00", "finished_at": "2026-11-10T00:01:00+00:00"})


def test_source_checksum_path_and_manifest(spark: SparkSession, tmp_path: Path) -> None:
    make_source(spark, tmp_path)
    store = AnalysisArtifacts(tmp_path)
    original = read_json(tmp_path / "runs/source/manifest.json")
    store.source("source")
    for change in ({"status": "pending"}, {"pipeline_version": "99"},
                   {"outputs": {"silver": "../outside"}},
                   {"counts": {**original["counts"], "silver_unique": 3}}):
        atomic_json(tmp_path / "runs/source/manifest.json", {**original, **change})
        with pytest.raises(ValueError):
            store.source("source")
    atomic_json(tmp_path / "runs/source/manifest.json", original)
    (tmp_path / "events.jsonl").write_text("corrupted")
    with pytest.raises(ValueError, match="checksum"):
        store.source("source")


def test_failure_manifest_on_bad_source(tmp_path: Path) -> None:
    engine = SparkAnalysis(AnalysisArtifacts(tmp_path))
    with pytest.raises(FileNotFoundError):
        engine.evaluate("missing", "failure", RuleConfig())
    assert read_json(tmp_path / "analysis-runs/failure/manifest.json")["status"] == "failed"


def test_wrong_feature_type_and_missing_target(spark: SparkSession) -> None:
    for schema, value in (("double", 1.0), ("decimal(12,3)", Decimal("1.000"))):
        data = spark.createDataFrame([("a", value)], f"event_id string, amount_decimal {schema}")
        with pytest.raises(ValueError, match="scale"):
            score_features(data, RuleConfig())
    data = frame(spark, [("a", "1.00", True)])
    predictions = score_features(data.select("event_id", "amount_decimal"), RuleConfig())
    with pytest.raises(ValueError):
        join_targets(predictions, data.select("event_id"))


def test_fixed_config_rejects_retuning(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="fixed default"):
        SparkAnalysis(AnalysisArtifacts(tmp_path)).evaluate(
            "source", "retune", RuleConfig(threshold_brl=Decimal("2000.00")))
    assert read_json(tmp_path / "analysis-runs/retune/manifest.json")["status"] == "failed"
