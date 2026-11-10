"""Vectorized exact-cent scoring and post-prediction Spark evaluation."""

from hashlib import sha256
from pathlib import Path
import os
import platform
import re
import time
from typing import Any

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from pixguard_ai.domain.analysis import RuleConfig, score_amount
from pixguard_ai.infrastructure.local.analysis_artifacts import (
    AnalysisArtifacts, WARNING, atomic_json, canonical_bytes, digest, file_record,
    inventory, protocol, read_json, utc_now,
)


def validate_keys(frame: DataFrame) -> int:
    """Require nonempty, nonnull unique string IDs, collecting only one aggregate."""
    if frame.schema["event_id"].dataType != T.StringType():
        raise ValueError("event_id must be a string")
    row = frame.agg(F.count("*").alias("n"), F.countDistinct("event_id").alias("unique"),
                    F.sum(F.when(F.col("event_id") == "", 1).otherwise(0)).alias("empty")).first()
    if row.n != row.unique or row.empty:
        raise ValueError("event_id must be unique, nonnull and nonempty")
    return row.n


def score_features(features: DataFrame, config: RuleConfig) -> DataFrame:
    """Accept only the allowlisted projection; keys never participate in the rule."""
    if features.columns != ["event_id", "amount_decimal"]:
        raise ValueError("scorer accepts only event_id and amount_decimal")
    amount_type = features.schema["amount_decimal"].dataType
    if not isinstance(amount_type, T.DecimalType) or amount_type.scale != 2:
        raise ValueError("amount_decimal must have exact Decimal scale 2")
    amount = F.col("amount_decimal")
    if features.filter(amount.isNull() | (amount < F.lit("0.01").cast("decimal(12,2)"))
                       | (amount > F.lit("1000000.00").cast("decimal(12,2)"))).limit(1).count():
        raise ValueError("amount_decimal outside payment contract")
    threshold = score_amount(config.threshold_brl, config).score_cents
    cents = (amount * F.lit(100)).cast("long")
    decision = cents >= F.lit(threshold)
    return features.select(
        "event_id", amount.cast("string").alias("amount_brl"), cents.alias("score_cents"),
        decision.alias("predicted_suspicious"), F.lit(str(config.threshold_brl)).alias("threshold_brl"),
        F.lit(config.operator).alias("operator"), F.lit(config.rule_id).alias("rule_id"),
        F.lit(config.rule_version).alias("rule_version"),
        F.when(decision, "AMOUNT_AT_OR_ABOVE_THRESHOLD").otherwise("AMOUNT_BELOW_THRESHOLD").alias("reason_code"),
        F.when(decision, "Valor igual ou superior ao limite da regra")
        .otherwise("Valor inferior ao limite da regra").alias("reason"),
    )


def join_targets(predictions: DataFrame, targets: DataFrame) -> DataFrame:
    """Join after persisted predictions, enforcing exact key coverage both ways."""
    if targets.columns != ["event_id", "is_suspicious"] or targets.schema["is_suspicious"].dataType != T.BooleanType():
        raise ValueError("evaluation target must be a boolean")
    n = validate_keys(predictions)
    if validate_keys(targets) != n or targets.filter(F.col("is_suspicious").isNull()).limit(1).count():
        raise ValueError("evaluation target is incomplete")
    if (predictions.join(targets, "event_id", "left_anti").limit(1).count()
            or targets.join(predictions, "event_id", "left_anti").limit(1).count()):
        raise ValueError("evaluation key coverage is incomplete")
    joined = predictions.join(targets, "event_id", "inner")
    p, y = F.col("predicted_suspicious"), F.col("is_suspicious")
    return joined.withColumn("cell", F.when(p & y, "TP").when(p & ~y, "FP")
                             .when(~p & y, "FN").otherwise("TN"))


def ratio(numerator: int, denominator: int, reason: str) -> dict[str, Any]:
    """Represent undefined values explicitly, never as NaN or invented zero."""
    return {"value": numerator / denominator if denominator else None,
            "defined": bool(denominator), "reason": None if denominator else reason}


def count_metrics(tp: int, fp: int, fn: int, tn: int) -> dict[str, Any]:
    """Calculate ratios from reconciled confusion counts."""
    n, positives, negatives, alerts = tp + fp + fn + tn, tp + fn, fp + tn, tp + fp
    return {"precision": ratio(tp, alerts, "no_alerts"),
            "recall": ratio(tp, positives, "no_positive_targets"),
            "f1": ratio(2 * tp, 2 * tp + fp + fn, "no_positive_targets_or_alerts"),
            "fpr": ratio(fp, negatives, "no_negative_targets"),
            "accuracy": ratio(tp + tn, n, "no_evaluable_rows"),
            "prevalence": ratio(positives, n, "no_evaluable_rows"),
            "alert_rate": ratio(alerts, n, "no_evaluable_rows")}


def evaluate_metrics(evaluation: DataFrame, source_count: int) -> dict[str, Any]:
    """Aggregate confusion cells and evaluate the raw ranking with exact Spark PR-AUC."""
    row = evaluation.agg(*[F.sum(F.when(F.col("cell") == cell, 1).otherwise(0)).alias(cell)
                            for cell in ("TP", "FP", "FN", "TN")]).first()
    tp, fp, fn, tn = (int(row[cell] or 0) for cell in ("TP", "FP", "FN", "TN"))
    n, positives, negatives = tp + fp + fn + tn, tp + fn, fp + tn
    if n != source_count:
        raise ValueError("confusion counts do not reconcile")
    metrics = count_metrics(tp, fp, fn, tn)
    if positives and negatives:
        # Contract cents <= 100,000,000 fit exactly in Double. Decisions already use integers.
        scores = evaluation.select(F.col("score_cents").cast("double").alias("score"),
                                   F.col("is_suspicious").cast("double").alias("label"))
        value = BinaryClassificationEvaluator(rawPredictionCol="score", labelCol="label",
                                               metricName="areaUnderPR", numBins=0).evaluate(scores)
        metrics["pr_auc"] = {"value": value, "defined": True, "reason": None}
    else:
        metrics["pr_auc"] = {"value": None, "defined": False, "reason": "requires_both_classes"}
    counts = {"source_silver": source_count, "predictions": n, "evaluation": n,
              "N": n, "P": positives, "Nneg": negatives, "alerts": tp + fp,
              "TP": tp, "FP": fp, "FN": fn, "TN": tn}
    baseline = count_metrics(0, 0, positives, negatives)
    baseline["pr_auc"] = {"value": None, "defined": False, "reason": "not_applicable_fixed_decision"}
    return {"counts": counts, "metrics": metrics,
            "always_negative": {"counts": {"TP": 0, "FP": 0, "FN": positives, "TN": negatives},
                                "metrics": baseline},
            "population_status": "no_evaluable_rows" if not n else (
                "both_classes" if positives and negatives else "single_class"),
            "evaluation_kind": "simulator_conformance", "warning": WARNING}


def new_session(name: str) -> SparkSession:
    """Create a bounded local session with UTC and deterministic shuffle sizing."""
    session = (SparkSession.builder.master("local[2]").appName(name)
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.shuffleExchange.maxThreadThreshold", "8")
               .config("spark.sql.broadcastExchange.maxThreadThreshold", "8")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false").getOrCreate())
    session.sparkContext.setLogLevel("WARN")
    return session


def resources() -> dict[str, Any]:
    """Read container cgroup measurements without presenting host RSS as Spark memory."""
    result: dict[str, Any] = {}
    for key, name in (("memory_peak_bytes", "memory.peak"), ("memory_limit_bytes", "memory.max")):
        try:
            value = Path("/sys/fs/cgroup", name).read_text().strip()
            result[key] = int(value) if value.isdigit() else None
        except OSError:
            result[key] = None
    result["measurement"] = "container_cgroup_v2; null means unavailable"
    return result


def image_provenance() -> dict[str, str | None]:
    """Record a launcher-supplied immutable image ID without accessing Docker's socket."""
    image_id = os.environ.get("PIXGUARD_IMAGE_DIGEST")
    if image_id is not None and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
        raise ValueError("PIXGUARD_IMAGE_DIGEST must be an immutable sha256 image ID")
    return {"image_digest": image_id,
            "image_digest_source": "container_launcher" if image_id else
            "unavailable: launcher did not supply digest; container has no Docker socket"}


class SparkAnalysis:
    """Compose the batch adapter and local artifact store without changing FASE3."""

    def __init__(self, artifacts: AnalysisArtifacts) -> None:
        """Store the filesystem boundary; acquire Spark only during a use case."""
        self.artifacts = artifacts

    def evaluate(self, source_run_id: str, analysis_run_id: str,
                 config: RuleConfig) -> dict[str, Any]:
        """Score, persist, then attach labels and publish a reconciled completed run."""
        started, started_at = time.monotonic(), utc_now()
        spark = None
        with self.artifacts.reserve(analysis_run_id) as run:
            try:
                if config != RuleConfig():
                    raise ValueError("this protocol only supports the fixed default rule")
                image_version = image_provenance()
                frozen = protocol(config)
                atomic_json(run / "protocol.json", frozen)
                source, silver_path, source_files = self.artifacts.source(source_run_id)
                source_manifest_path = self.artifacts.root / "runs" / source_run_id / "manifest.json"
                source_hash = digest(source_manifest_path)
                # Preserve exact source bytes so this copy retains the original file checksum.
                (run / "source-manifest.json").write_bytes(source_manifest_path.read_bytes())
                if (read_json(run / "source-manifest.json") != source
                        or digest(run / "source-manifest.json") != source_hash):
                    raise ValueError("source manifest changed while copying")
                atomic_json(run / "source-files.json", {"files": source_files,
                    "provenance": self.artifacts.provenance(source),
                    "checksum_scope": "snapshot_at_analysis_start; FASE3 has no historical Silver digest"})
                spark = new_session(f"pixguard-analysis-{analysis_run_id}")
                silver = spark.read.parquet(str(silver_path))
                n = validate_keys(silver)
                if n != source["counts"]["silver_unique"]:
                    raise ValueError("source count disagrees with manifest")
                if (silver.schema["label_is_synthetic"].dataType != T.BooleanType()
                        or silver.filter(F.col("label_is_synthetic").isNull()
                                         | ~F.col("label_is_synthetic")).limit(1).count()):
                    raise ValueError("only explicitly synthetic rows are eligible")
                predictions = score_features(silver.select("event_id", "amount_decimal"), config)
                predictions.write.mode("error").parquet(str(run / "predictions"))
                persisted = spark.read.parquet(str(run / "predictions"))
                if validate_keys(persisted) != n:
                    raise ValueError("persisted predictions count differs")
                # The target is first selected only after predictions have reached durable storage.
                targets = silver.select("event_id", F.col("ground_truth.is_suspicious").alias("is_suspicious"))
                evaluation = join_targets(persisted, targets)
                evaluation.write.mode("error").parquet(str(run / "evaluation"))
                evaluated = spark.read.parquet(str(run / "evaluation")).cache()
                if validate_keys(evaluated) != n:
                    raise ValueError("persisted evaluation count differs")
                metrics = evaluate_metrics(evaluated, n)
                atomic_json(run / "metrics.json", metrics)
                examples = [row.asDict() for row in persisted.orderBy(F.desc("score_cents"), "event_id")
                            .limit(20).collect()]
                atomic_json(run / "dashboard-summary.json", {
                    "label_is_synthetic": True, "counts": metrics["counts"],
                    "metrics": metrics["metrics"], "examples": examples, "warning": WARNING})
                final_source, _, final_files = self.artifacts.source(source_run_id)
                if (source != final_source or source_files != final_files
                        or source_hash != digest(source_manifest_path)):
                    raise ValueError("source changed during analysis")
                artifacts = [file_record(run, run / name) for name in (
                    "protocol.json", "source-manifest.json", "source-files.json", "metrics.json", "dashboard-summary.json")]
                artifacts += inventory(run, run / "predictions") + inventory(run, run / "evaluation")
                package_root = Path(__file__).parents[2]
                code_digest = sha256()
                for path in sorted(package_root.rglob("*.py")):
                    code_digest.update(str(path.relative_to(package_root)).encode())
                    code_digest.update(path.read_bytes())
                manifest = {"analysis_manifest_version": 1, "analysis_run_id": analysis_run_id,
                    "source_run_id": source_run_id, "status": "completed", "label_is_synthetic": True,
                    "protocol_id": frozen["protocol_id"], "rule": config.to_dict(),
                    "config_sha256": sha256(canonical_bytes(frozen)).hexdigest(),
                    "source_manifest_sha256": source_hash, "source_counts": source["counts"],
                    "counts": metrics["counts"], "population_status": metrics["population_status"],
                    "reconciliation": {"passed": True, "formula": "Silver = predictions = evaluation = TP+FP+FN+TN"},
                    "artifacts": artifacts, "outputs": {name: name for name in ("predictions", "evaluation")},
                    "dashboard": {"summary_path": "dashboard-summary.json", "metrics_path": "metrics.json"},
                    "versions": {"python": platform.python_version(), "spark": spark.version,
                                 "code_sha256": code_digest.hexdigest(), **image_version},
                    "evaluator": frozen["pr_auc"], "started_at": started_at, "finished_at": utc_now(),
                    "elapsed_seconds": round(time.monotonic() - started, 3), "resources": resources(),
                    "artifact_bytes": sum(record["size_bytes"] for record in artifacts),
                    "warning": WARNING}
                spark.stop()
                spark = None
                atomic_json(run / "manifest.json", manifest)
                return manifest
            finally:
                if spark is not None:
                    spark.stop()

    def compare(self, left: str, right: str) -> dict[str, Any]:
        """Compare full datasets in both directions and deterministic metric/config JSON."""
        left_dir, left_manifest = self.artifacts.completed(left)
        right_dir, right_manifest = self.artifacts.completed(right)
        for key in ("config_sha256", "counts", "source_manifest_sha256", "evaluator"):
            if left_manifest[key] != right_manifest[key]:
                raise ValueError(f"analysis difference: {key}")
        for filename in ("protocol.json", "metrics.json", "dashboard-summary.json", "source-files.json"):
            if read_json(left_dir / filename) != read_json(right_dir / filename):
                raise ValueError(f"analysis difference: {filename}")
        spark = new_session("pixguard-analysis-compare")
        try:
            for name in ("predictions", "evaluation"):
                a, b = (spark.read.parquet(str(path / name)) for path in (left_dir, right_dir))
                if (a.schema != b.schema or a.exceptAll(b).limit(1).count()
                        or b.exceptAll(a).limit(1).count()):
                    raise ValueError(f"analysis difference: {name}")
        finally:
            spark.stop()
        return {"equivalent": True, "left": left, "right": right,
                "counts": left_manifest["counts"], "config_sha256": left_manifest["config_sha256"]}
