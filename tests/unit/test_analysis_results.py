"""Security and reconciliation tests for locally persisted analysis artifacts."""

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import pytest

from pixguard_ai.infrastructure.local.analysis_results import load_latest_analysis_results


COUNTS = {"TP": 50, "FP": 0, "FN": 0, "TN": 950, "N": 1000, "P": 50,
          "Nneg": 950, "alerts": 50, "source_silver": 1000, "predictions": 1000,
          "evaluation": 1000}
WARNING = "Regra baseada nas faixas artificiais do gerador; não comprova detecção de fraude."


def _metric(value: float | None, reason: str | None = None) -> dict[str, object]:
    return {"defined": value is not None, "reason": reason, "value": value}


def _write_run(root: Path, run_id: str = "run-001", *, status: str = "completed",
               finished_at: str | None = None, mutate: str | None = None) -> Path:
    """Create a complete valid fixture and return its run directory."""
    run = root / "analysis-runs" / run_id
    run.mkdir(parents=True)
    rule = {"feature_columns": ["amount_decimal"], "operator": ">=", "rule_id": "amount_threshold_v1",
            "rule_version": "1.0.0", "threshold_brl": "10000.00"}
    protocol = {"protocol_id": "fixed_simulator_conformance_v1", "rule": rule,
                "tuning": "none", "blind": False, "generalization_claim": False}
    if mutate == "retuned":
        protocol["rule"] = {**rule, "threshold_brl": "9000.00"}
    protocol_path = run / "protocol.json"
    protocol_path.write_text(json.dumps(protocol))
    source_path = run / "source-manifest.json"
    source_path.write_text('{"run_id":"source-001"}')
    (run / "source-files.json").write_text('{"files":[]}')
    metrics_values = {
        "precision": _metric(1.0), "recall": _metric(1.0), "f1": _metric(1.0),
        "fpr": _metric(0.0), "accuracy": _metric(1.0), "prevalence": _metric(0.05),
        "alert_rate": _metric(0.05), "pr_auc": _metric(1.0),
    }
    baseline_values = {
        "precision": _metric(None, "no_alerts"), "recall": _metric(0.0), "f1": _metric(0.0),
        "fpr": _metric(0.0), "accuracy": _metric(0.95), "prevalence": _metric(0.05),
        "alert_rate": _metric(0.0), "pr_auc": _metric(None, "not_applicable_fixed_decision"),
    }
    metrics = {"counts": COUNTS, "metrics": metrics_values,
               "always_negative": {"counts": {"TP": 0, "FP": 0, "FN": 50, "TN": 950},
                                   "metrics": baseline_values}}
    if mutate == "metrics":
        metrics["metrics"]["precision"] = _metric(0.2)
    (run / "metrics.json").write_text(json.dumps(metrics))
    summary = {"label_is_synthetic": True, "counts": COUNTS,
               "metrics": metrics["metrics"], "examples": [], "warning": WARNING}
    if mutate == "too_many_examples":
        summary["examples"] = [{"event_id": "synthetic-1", "rule_id": "amount_threshold_v1",
                                "rule_version": "1.0.0", "threshold_brl": "10000.00",
                                "reason_code": "AMOUNT_AT_OR_ABOVE_THRESHOLD",
                                "predicted_suspicious": True}] * 21
    (run / "dashboard-summary.json").write_text(json.dumps(summary))
    (run / "predictions").mkdir()
    (run / "predictions" / "part.parquet").write_bytes(b"predictions")
    artifacts = []
    for path in sorted(p for p in run.rglob("*") if p.is_file()):
        raw = path.read_bytes()
        artifacts.append({"path": path.relative_to(run).as_posix(),
                          "sha256": hashlib.sha256(raw).hexdigest(), "size_bytes": len(raw)})
    if mutate == "checksum":
        artifacts[-1]["sha256"] = "0" * 64
    if mutate == "incomplete":
        artifacts = [item for item in artifacts if item["path"] != "source-files.json"]
    now = finished_at or datetime.now(timezone.utc).isoformat()
    manifest = {
        "analysis_manifest_version": 1, "analysis_run_id": run_id, "status": status,
        "started_at": now, "finished_at": now, "source_run_id": "source-001",
        "source_manifest_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "config_sha256": "a" * 64, "protocol_id": "fixed_simulator_conformance_v1",
        "rule": rule, "label_is_synthetic": True, "counts": COUNTS, "artifacts": artifacts,
        "dashboard": {"metrics_path": "metrics.json", "summary_path": "dashboard-summary.json"},
        "warning": WARNING, "reconciliation": {"passed": True},
    }
    if mutate == "path":
        artifacts[0]["path"] = "../../outside.json"
    if mutate == "reference":
        manifest["dashboard"]["metrics_path"] = "../../metrics.json"
    if mutate == "timestamp":
        manifest["finished_at"] = "2026-01-01T10:00:00-03:00"
    if mutate == "duplicate-json":
        (run / "manifest.json").write_text('{"status":"completed","status":"failed"}')
    elif mutate == "invalid-json":
        (run / "manifest.json").write_text("{")
    else:
        (run / "manifest.json").write_text(json.dumps(manifest))
    if mutate == "oversize":
        with (run / "manifest.json").open("ab") as stream:
            stream.write(b" " * 70_000)
    return run


def test_loads_latest_valid_analysis_and_ignores_pending(tmp_path: Path) -> None:
    """Select the newest valid completed run and leave pending runs unselected."""
    _write_run(tmp_path, "older", finished_at="2026-11-10T10:00:00+00:00")
    pending = tmp_path / "analysis-runs" / "pending-002"
    pending.mkdir()
    (pending / "pending.json").write_text('{"status":"pending"}')
    _write_run(tmp_path, "newer", finished_at="2026-11-10T11:00:00+00:00")

    snapshot = load_latest_analysis_results(tmp_path)

    assert snapshot.manifest["analysis_run_id"] == "newer"
    assert snapshot.metrics["metrics"]["precision"]["value"] == 1.0
    assert snapshot.summary["examples"] == []
    assert snapshot.newer_failure is None


@pytest.mark.parametrize("mutation", ["metrics", "checksum", "path", "reference", "timestamp",
                                       "duplicate-json", "invalid-json", "oversize", "incomplete",
                                       "retuned", "too_many_examples"])
def test_rejects_invalid_artifact_manifests(tmp_path: Path, mutation: str) -> None:
    """Do not expose corrupt JSON, unsafe paths, hashes, timestamps, or metrics."""
    _write_run(tmp_path, mutate=mutation)

    snapshot = load_latest_analysis_results(tmp_path)

    assert snapshot.manifest is None
    assert snapshot.error is True


def test_symlink_artifact_is_rejected(tmp_path: Path) -> None:
    """Reject artifact symlinks even when their targets remain inside the volume."""
    run = _write_run(tmp_path)
    artifact = run / "source-files.json"
    target = run / "actual-source-files.json"
    artifact.rename(target)
    artifact.symlink_to(target.name)
    manifest_path = run / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["artifacts"] = [
        {"path": "source-files.json", "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
         "size_bytes": target.stat().st_size}
        if item["path"] == "source-files.json" else item for item in manifest["artifacts"]]
    manifest_path.write_text(json.dumps(manifest))

    snapshot = load_latest_analysis_results(tmp_path)

    assert snapshot.manifest is None
    assert snapshot.error is True


def test_newer_failure_warns_but_keeps_last_good_run(tmp_path: Path) -> None:
    """Show the last good results with a warning for a subsequent failed run."""
    _write_run(tmp_path, "good-001", finished_at="2026-11-10T10:00:00+00:00")
    failed = tmp_path / "analysis-runs" / "failed-002"
    failed.mkdir()
    (failed / "manifest.json").write_text(json.dumps({
        "status": "failed", "failed_at": "2026-11-10T11:00:00+00:00", "analysis_run_id": "failed-002"}))

    snapshot = load_latest_analysis_results(tmp_path)

    assert snapshot.manifest["analysis_run_id"] == "good-001"
    assert snapshot.newer_failure == "failed-002"


def test_undefined_metrics_stay_null_in_verified_run(tmp_path: Path) -> None:
    """Preserve an undefined baseline precision and its explanation in the contract."""
    run = _write_run(tmp_path)

    snapshot = load_latest_analysis_results(tmp_path)

    undefined = snapshot.metrics["always_negative"]["metrics"]["precision"]
    assert undefined == {"defined": False, "reason": "no_alerts", "value": None}
    assert snapshot.manifest["rule"]["threshold_brl"] == "10000.00"
    assert "não comprova detecção de fraude" in snapshot.manifest["warning"].lower()
