"""Presentation tests for Kafka stage states in the dashboard."""

from pixguard_ai.infrastructure.local.streaming_results import StreamingSnapshot
from pixguard_ai.interfaces.dashboard import render_streaming_results


class Dashboard:
    """Collect Streamlit calls as bounded text and metrics."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.metrics: list[str] = []

    def __getattr__(self, name: str):
        return lambda value, *args, **kwargs: self.lines.append(str(value))

    def columns(self, count: int) -> list[object]:
        return [Metric(self.metrics) for _ in range(count)]


class Metric:
    def __init__(self, target: list[str]) -> None:
        self.target = target

    def metric(self, label: str, value: str) -> None:
        self.target.append(f"{label}: {value}")


def test_renders_separate_publication_capture_and_snapshot_counts() -> None:
    dashboard = Dashboard()
    snapshot = StreamingSnapshot(
        publication={
            "source_batch_id": "batch-1",
            "status": "completed",
            "total": 100,
            "confirmed": 100,
            "uncertain": 0,
            "not_sent": 0,
        },
        capture={
            "capture_id": "capture-1",
            "status": "completed",
            "topic": "pixguard.payment_event_v1",
            "partition": 0,
            "start": 0,
            "end": 100,
            "count": 100,
        },
        snapshot={
            "snapshot_id": "snapshot-1",
            "capture_id": "capture-1",
            "finished_at": "2026-11-20T10:00:22+00:00",
            "counts": {
                "physical": 100,
                "exported": 99,
                "duplicates": 1,
                "transport_quarantine": 0,
                "contract_quarantine": 0,
            },
        },
        message="Manifestos de transporte Kafka disponíveis.",
    )

    render_streaming_results(dashboard, snapshot)

    text = " ".join(dashboard.lines + dashboard.metrics)
    assert "batch-1" in text
    assert "capture-1" in text
    assert "snapshot-1" in text
    assert "Confirmados: 100" in text
    assert "Físicas: 100" in text
    assert "lag não é estimado" in text.lower()


def test_missing_or_failed_state_is_explicit_and_sanitized() -> None:
    dashboard = Dashboard()
    render_streaming_results(
        dashboard,
        StreamingSnapshot(None, None, None, "Nenhum transporte Kafka disponível."),
    )
    assert "Nenhum transporte" in " ".join(dashboard.lines)

    dashboard = Dashboard()
    publication = {
        "source_batch_id": "batch-failed",
        "status": "failed",
        "total": 10,
        "confirmed": 2,
        "uncertain": 3,
        "not_sent": 5,
        "error": "broker\x00payload-secret",
    }
    render_streaming_results(
        dashboard,
        StreamingSnapshot(publication, None, None, "Manifestos disponíveis."),
    )
    text = " ".join(dashboard.lines)
    assert "Falha registrada no manifesto" in text
    assert "payload-secret" not in text
