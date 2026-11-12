"""Presentation tests for the FASE 4 dashboard section."""

from types import SimpleNamespace

from pixguard_ai.infrastructure.local.analysis_results import AnalysisSnapshot
from pixguard_ai.interfaces.dashboard import _format_metric, render_analysis_results


class _Column:
    """Collect metric labels and values as a lightweight Streamlit substitute."""

    def __init__(self, target: list[str]) -> None:
        self.target = target

    def metric(self, label: str, value: str) -> None:
        self.target.append(f"{label}: {value}")


class _Dashboard:
    """Collect text and tables rendered by the dashboard function."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.metrics: list[str] = []

    def __getattr__(self, name: str):
        return lambda value, *args, **kwargs: self.lines.append(str(value))

    def columns(self, count: int) -> list[_Column]:
        return [_Column(self.metrics) for _ in range(count)]

    def dataframe(self, value, **kwargs) -> None:
        self.lines.append(str(value))


def _snapshot() -> AnalysisSnapshot:
    """Return a small validated fixture ready for the presentation layer."""
    rule = {"rule_id": "amount_threshold_v1", "rule_version": "1.0.0",
            "operator": ">=", "threshold_brl": "10000.00"}
    counts = {"N": 1000, "alerts": 50, "P": 50, "Nneg": 950, "TP": 50,
              "FP": 0, "FN": 0, "TN": 950}
    metrics = {name: {"defined": True, "value": value, "reason": None}
               for name, value in {"precision": 1.0, "recall": 1.0, "f1": 1.0,
                                   "fpr": 0.0, "pr_auc": 1.0, "accuracy": 1.0,
                                   "prevalence": 0.05, "alert_rate": 0.05}.items()}
    baseline = {name: {"defined": True, "value": value, "reason": None}
                for name, value in {"recall": 0.0, "f1": 0.0, "fpr": 0.0,
                                    "accuracy": 0.95, "prevalence": 0.05,
                                    "alert_rate": 0.0}.items()}
    baseline["precision"] = {"defined": False, "value": None, "reason": "no_alerts"}
    baseline["pr_auc"] = {"defined": False, "value": None,
                          "reason": "not_applicable_fixed_decision"}
    summary = {"examples": [{"event_id": "synthetic-1", "amount_brl": "10000.00",
                             "predicted_suspicious": True, "reason": "At threshold",
                             "reason_code": "AMOUNT_AT_OR_ABOVE_THRESHOLD"}]}
    manifest = {"analysis_run_id": "eval-004", "source_run_id": "batch-100k-001",
                "finished_at": "2026-11-10T11:38:51.826540+00:00", "rule": rule,
                "counts": {**counts, "source_silver": 1000, "predictions": 1000, "evaluation": 1000},
                "elapsed_seconds": 9.917, "warning": "não comprova detecção de fraude"}
    return AnalysisSnapshot(manifest, {"metrics": metrics,
                                       "always_negative": {"metrics": baseline,
                                                           "counts": {"TP": 0, "FP": 0}}},
                            summary, "Resultado da análise FASE 4 concluída e validada.")


def test_dashboard_renders_counts_matrix_metrics_and_limit_warning() -> None:
    """Expose source, rule, confusion matrix, null reasons, and synthetic limitation."""
    dashboard = _Dashboard()

    render_analysis_results(dashboard, _snapshot())

    all_text = " ".join(dashboard.lines + dashboard.metrics)
    assert "eval-004" in all_text
    assert "batch-100k-001" in all_text
    assert "10.000,00" in all_text
    assert "não comprova detecção de fraude" in all_text
    assert "indefinida: nenhuma previsão de alerta" in all_text
    assert "Alerta previsto" in all_text
    assert "synthetic-1" in all_text


def test_missing_snapshot_shows_state_without_invented_zeroes() -> None:
    """Do not turn missing FASE 4 output into zero-valued metrics."""
    dashboard = _Dashboard()
    snapshot = AnalysisSnapshot(None, None, None, "Nenhuma análise válida disponível.")

    render_analysis_results(dashboard, snapshot)

    assert dashboard.lines == ["Nenhuma análise válida disponível.",
                               "A análise é calculada separadamente em Spark e publicada como artefato imutável."]
    assert dashboard.metrics == []


def test_undefined_metrics_have_readable_reasons() -> None:
    """Translate the exact undefined reasons emitted by the Spark evaluator."""
    cases = {
        "no_alerts": "indefinida: nenhuma previsão de alerta",
        "no_positive_targets": "indefinida: sem exemplos positivos",
        "no_positive_targets_or_alerts": "indefinida: sem positivos e sem alertas",
        "no_negative_targets": "indefinida: sem exemplos negativos",
        "no_evaluable_rows": "indefinida: população vazia",
        "requires_both_classes": "indefinida: requer exemplos positivos e negativos",
        "not_applicable_fixed_decision": "não aplicável a uma decisão fixa",
    }

    for reason, expected in cases.items():
        assert _format_metric({"metric": {"defined": False, "value": None,
                                          "reason": reason}}, "metric") == expected
