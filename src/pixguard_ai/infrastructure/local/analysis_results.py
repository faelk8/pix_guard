"""Validate bounded analysis artifacts for the base Streamlit application."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any


MAX_MANIFEST_BYTES = 65_536
MAX_JSON_BYTES = 262_144
MAX_RUN_DIRECTORIES = 100_000
MAX_ARTIFACTS = 10_000
MAX_EXAMPLES = 20
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_METRIC_NAMES = ("precision", "recall", "f1", "fpr", "accuracy", "prevalence", "alert_rate", "pr_auc")


@dataclass(frozen=True)
class AnalysisSnapshot:
    """Represent the newest completed valid analysis without loading Parquet rows."""

    manifest: dict[str, Any] | None
    metrics: dict[str, Any] | None
    summary: dict[str, Any] | None
    message: str
    error: bool = False
    newer_failure: str | None = None


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate JSON keys instead of silently choosing one value."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _read_json(path: Path, limit: int) -> Any:
    """Read a size-bounded JSON object from a regular file."""
    with path.open("rb") as stream:
        payload = stream.read(limit + 1)
    if len(payload) > limit:
        raise ValueError("JSON file exceeds read limit")
    return json.loads(payload, object_pairs_hook=_unique_object)


def _timestamp(value: object) -> datetime:
    """Parse a timestamp that explicitly represents UTC."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must use UTC")
    return parsed


def _safe_path(root: Path, relative: object) -> Path:
    """Resolve a relative path while rejecting traversal and symlink components."""
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("artifact path must be relative")
    parts = Path(relative).parts
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError("artifact path contains traversal")
    candidate = root
    for part in parts:
        candidate = candidate / part
        if candidate.is_symlink():
            raise ValueError("symlink artifact paths are not accepted")
    resolved = candidate.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise ValueError("artifact path escapes run directory")
    return resolved


def _digest(path: Path) -> tuple[str, int]:
    """Hash a file in bounded chunks and return its size."""
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            hasher.update(chunk)
    return hasher.hexdigest(), size


def _integer(value: object) -> bool:
    """Accept nonnegative JSON integers but reject booleans."""
    return type(value) is int and value >= 0


def _metric(value: object, name: str, expected: float | None, reason: str | None = None) -> bool:
    """Check a metric's defined/null representation and expected value."""
    if not isinstance(value, dict) or type(value.get("defined")) is not bool:
        return False
    if expected is None:
        return (value["defined"] is False and value.get("value") is None
                and isinstance(value.get("reason"), str) and bool(value["reason"]))
    raw = value.get("value")
    return (value["defined"] is True and value.get("reason") is None
            and type(raw) in (int, float) and math.isfinite(raw)
            and math.isclose(float(raw), expected, rel_tol=1e-9, abs_tol=1e-9))


def _validate_metrics(metrics: object, counts: dict[str, int]) -> dict[str, Any]:
    """Validate confusion counts and recompute every reported scalar metric."""
    if not isinstance(metrics, dict) or metrics.get("counts") != counts:
        raise ValueError("metrics counts do not match manifest")
    values = metrics.get("metrics")
    baseline = metrics.get("always_negative")
    if not isinstance(values, dict) or not isinstance(baseline, dict):
        raise ValueError("metrics contract is incomplete")
    if baseline.get("counts") != {"TP": 0, "FP": 0, "FN": counts["P"], "TN": counts["Nneg"]}:
        raise ValueError("always-negative baseline counts are inconsistent")
    base_values = baseline.get("metrics")
    if not isinstance(base_values, dict):
        raise ValueError("baseline metrics are incomplete")

    tp, fp, fn, tn = (counts[key] for key in ("TP", "FP", "FN", "TN"))
    n, positives, negatives, alerts = counts["N"], counts["P"], counts["Nneg"], counts["alerts"]
    precision = tp / alerts if alerts else None
    recall = tp / positives if positives else None
    f1 = 2 * tp / (2 * tp + fp + fn) if (2 * tp + fp + fn) else None
    fpr = fp / negatives if negatives else None
    accuracy = (tp + tn) / n if n else None
    prevalence = positives / n if n else None
    alert_rate = alerts / n if n else None
    expected = {"precision": precision, "recall": recall, "f1": f1, "fpr": fpr,
                "accuracy": accuracy, "prevalence": prevalence, "alert_rate": alert_rate}
    for name, result in expected.items():
        if not _metric(values.get(name), name, result):
            raise ValueError(f"metric {name} does not match confusion matrix")
    pr_auc = values.get("pr_auc")
    if positives and negatives:
        if (not isinstance(pr_auc, dict) or pr_auc.get("defined") is not True
                or type(pr_auc.get("value")) not in (int, float)
                or not math.isfinite(pr_auc["value"]) or not 0 <= pr_auc["value"] <= 1
                or pr_auc.get("reason") is not None):
            raise ValueError("PR-AUC is invalid for a two-class population")
    elif not _metric(pr_auc, "pr_auc", None):
        raise ValueError("PR-AUC must be undefined without both classes")

    base_expected = {
        "precision": None,
        "recall": 0.0 if positives else None,
        "f1": 0.0 if positives else None,
        "fpr": 0.0 if negatives else None,
        "accuracy": negatives / n if n else None,
        "prevalence": positives / n if n else None,
        "alert_rate": 0.0 if n else None,
        "pr_auc": None,
    }
    for name, result in base_expected.items():
        if not _metric(base_values.get(name), name, result):
            raise ValueError(f"baseline metric {name} is inconsistent")
    return metrics


def _validate_completed(root: Path, run_dir: Path, value: object) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], datetime]:
    """Verify completed manifest, all artifact hashes, protocol, metrics, and summary."""
    if not isinstance(value, dict):
        raise ValueError("manifest must be an object")
    required = {"analysis_manifest_version", "analysis_run_id", "status", "finished_at", "started_at",
                "source_run_id", "source_manifest_sha256", "config_sha256", "protocol_id", "rule",
                "label_is_synthetic", "counts", "artifacts", "dashboard", "warning", "reconciliation"}
    if (not required <= value.keys() or type(value["analysis_manifest_version"]) is not int
            or value["analysis_manifest_version"] != 1 or value["status"] != "completed"
            or value["analysis_run_id"] != run_dir.name or _RUN_ID.fullmatch(run_dir.name) is None
            or value["label_is_synthetic"] is not True or value["protocol_id"] != "fixed_simulator_conformance_v1"):
        raise ValueError("completed manifest identity or schema is invalid")
    started_at, finished_at = _timestamp(value["started_at"]), _timestamp(value["finished_at"])
    if finished_at < started_at:
        raise ValueError("manifest timestamps are inconsistent")
    if (not isinstance(value["source_run_id"], str) or _RUN_ID.fullmatch(value["source_run_id"]) is None
            or not isinstance(value["source_manifest_sha256"], str)
            or _SHA256.fullmatch(value["source_manifest_sha256"]) is None
            or not isinstance(value["config_sha256"], str) or _SHA256.fullmatch(value["config_sha256"]) is None
            or value["reconciliation"].get("passed") is not True):
        raise ValueError("manifest provenance or reconciliation is invalid")
    rule = value["rule"]
    if (not isinstance(rule, dict) or rule.get("rule_id") != "amount_threshold_v1"
            or rule.get("rule_version") != "1.0.0" or rule.get("threshold_brl") != "10000.00"
            or rule.get("operator") != ">="):
        raise ValueError("unexpected or retuned rule")

    counts = value["counts"]
    keys = {"TP", "FP", "FN", "TN", "N", "P", "Nneg", "alerts", "source_silver", "predictions", "evaluation"}
    if not isinstance(counts, dict) or set(counts) != keys or not all(_integer(v) for v in counts.values()):
        raise ValueError("manifest counts are invalid")
    if (counts["source_silver"] != counts["predictions"] or counts["predictions"] != counts["evaluation"]
            or counts["N"] != counts["evaluation"] or counts["N"] != counts["TP"] + counts["FP"] + counts["FN"] + counts["TN"]
            or counts["P"] != counts["TP"] + counts["FN"] or counts["Nneg"] != counts["TN"] + counts["FP"]
            or counts["alerts"] != counts["TP"] + counts["FP"]):
        raise ValueError("manifest counts do not reconcile")

    artifacts = value["artifacts"]
    if not isinstance(artifacts, list) or not artifacts or len(artifacts) > MAX_ARTIFACTS:
        raise ValueError("artifact inventory is invalid")
    paths: set[str] = set()
    for record in artifacts:
        if not isinstance(record, dict):
            raise ValueError("artifact record is invalid")
        relative, expected_hash, expected_size = record.get("path"), record.get("sha256"), record.get("size_bytes")
        if (not isinstance(relative, str) or relative in paths or not isinstance(expected_hash, str)
                or _SHA256.fullmatch(expected_hash) is None or not _integer(expected_size)):
            raise ValueError("artifact metadata is invalid")
        paths.add(relative)
        artifact = _safe_path(run_dir, relative)
        if not artifact.is_file():
            raise ValueError("artifact must be a regular file")
        actual_hash, actual_size = _digest(artifact)
        if actual_hash != expected_hash or actual_size != expected_size:
            raise ValueError("artifact checksum or size mismatch")
    expected_artifacts = {"protocol.json", "source-manifest.json", "source-files.json", "metrics.json", "dashboard-summary.json"}
    if not expected_artifacts <= paths:
        raise ValueError("required artifacts are missing")
    if hashlib.sha256((_safe_path(run_dir, "source-manifest.json").read_bytes())).hexdigest() != value["source_manifest_sha256"]:
        raise ValueError("source manifest hash does not match")

    protocol = _read_json(_safe_path(run_dir, "protocol.json"), MAX_JSON_BYTES)
    if (not isinstance(protocol, dict) or protocol.get("protocol_id") != value["protocol_id"]
            or protocol.get("tuning") != "none" or protocol.get("blind") is not False
            or protocol.get("generalization_claim") is not False or protocol.get("rule") != rule):
        raise ValueError("analysis protocol is invalid")
    metrics = _read_json(_safe_path(run_dir, "metrics.json"), MAX_JSON_BYTES)
    validated_metrics = _validate_metrics(metrics, counts)
    summary = _read_json(_safe_path(run_dir, "dashboard-summary.json"), MAX_JSON_BYTES)
    if (not isinstance(summary, dict) or summary.get("label_is_synthetic") is not True
            or summary.get("counts") != counts or summary.get("metrics") != validated_metrics["metrics"]
            or not isinstance(summary.get("examples"), list) or len(summary["examples"]) > MAX_EXAMPLES):
        raise ValueError("dashboard summary is invalid")
    examples = summary["examples"]
    for example in examples:
        if (not isinstance(example, dict) or not isinstance(example.get("event_id"), str)
                or example.get("rule_id") != rule["rule_id"] or example.get("rule_version") != rule["rule_version"]
                or example.get("threshold_brl") != rule["threshold_brl"]
                or example.get("reason_code") not in {"AMOUNT_AT_OR_ABOVE_THRESHOLD", "AMOUNT_BELOW_THRESHOLD"}
                or type(example.get("predicted_suspicious")) is not bool):
            raise ValueError("dashboard example is invalid")
    if not isinstance(value["warning"], str) or "não comprova detecção de fraude" not in value["warning"].lower():
        raise ValueError("synthetic limitation warning is missing")
    dashboard = value["dashboard"]
    if not isinstance(dashboard, dict) or dashboard.get("metrics_path") != "metrics.json" or dashboard.get("summary_path") != "dashboard-summary.json":
        raise ValueError("dashboard references are invalid")
    return value, validated_metrics, summary, finished_at


def load_latest_analysis_results(data_root: Path) -> AnalysisSnapshot:
    """Select newest completed valid run, ignoring pending and failed outputs."""
    try:
        if data_root.is_symlink():
            raise ValueError("data root cannot be a symlink")
        root = data_root.resolve(strict=True)
        runs = root / "analysis-runs"
        if not runs.exists():
            return AnalysisSnapshot(None, None, None, "Ainda não há análises FASE 4 concluídas.")
        if runs.is_symlink() or not runs.is_dir() or not runs.resolve(strict=True).is_relative_to(root):
            raise ValueError("analysis-runs directory is invalid")
        directories = [path for path in runs.iterdir() if path.is_dir() and not path.is_symlink()]
        if len(directories) > MAX_RUN_DIRECTORIES:
            raise ValueError("too many analysis run directories")
    except FileNotFoundError:
        return AnalysisSnapshot(None, None, None, "Ainda não há análises FASE 4 concluídas.")
    except (OSError, ValueError, RuntimeError):
        return AnalysisSnapshot(None, None, None, "Diretório de análises inválido ou indisponível.", True)

    completed: list[tuple[datetime, dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    invalid_count = 0
    failures: list[tuple[datetime, str]] = []
    for run_dir in directories:
        path = run_dir / "manifest.json"
        if path.is_symlink() or not path.is_file():
            continue
        try:
            manifest = _read_json(path, MAX_MANIFEST_BYTES)
            if isinstance(manifest, dict) and manifest.get("status") == "failed":
                raw = manifest.get("failed_at", manifest.get("finished_at", manifest.get("started_at")))
                if raw is None:
                    raw = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
                failures.append((_timestamp(raw), run_dir.name))
                continue
            if isinstance(manifest, dict) and manifest.get("status") != "completed":
                continue
            valid, metrics, summary, finished_at = _validate_completed(root, run_dir, manifest)
            completed.append((finished_at, valid, metrics, summary))
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError,
                OverflowError, RuntimeError, AttributeError):
            invalid_count += 1
    if not completed:
        if failures:
            _, failed_id = max(failures)
            return AnalysisSnapshot(None, None, None, f"A análise {failed_id} falhou; ainda não há resultado válido.", True)
        if invalid_count:
            return AnalysisSnapshot(None, None, None, "Nenhum manifesto concluído válido foi encontrado.", True)
        return AnalysisSnapshot(None, None, None, "Ainda não há análises FASE 4 concluídas.")

    finished_at, manifest, metrics, summary = max(completed, key=lambda item: item[0])
    newer_failure = None
    if failures:
        failed_at, failed_id = max(failures)
        if failed_at > finished_at:
            newer_failure = failed_id
    message = "Resultado da análise FASE 4 concluída e validada."
    if invalid_count:
        message += f" {invalid_count} execução(ões) inválida(s) foram ignoradas."
    return AnalysisSnapshot(manifest, metrics, summary, message, bool(invalid_count), newer_failure)
