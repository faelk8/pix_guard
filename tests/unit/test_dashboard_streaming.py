"""Presentation tests for Kafka stage states in the dashboard."""

import json
from pathlib import Path

from pixguard_ai.infrastructure.local.streaming_results import StreamingSnapshot
from pixguard_ai.interfaces.dashboard import render_streaming_fragment, render_streaming_results


class Dashboard:
    """Collect Streamlit calls as bounded text and metrics."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.metrics: list[str] = []

    def __getattr__(self, name: str):
        return lambda value, *args, **kwargs: self.lines.append(str(value))

    def metric(self, label: str, value: str) -> None:
        self.metrics.append(f"{label}: {value}")

    def columns(self, count: int) -> list[object]:
        return [Metric(self.metrics) for _ in range(count)]


class Metric:
    def __init__(self, target: list[str]) -> None:
        self.target = target

    def metric(self, label: str, value: str) -> None:
        self.target.append(f"{label}: {value}")


class FragmentDashboard(Dashboard):
    """Capture a Streamlit fragment callback for deterministic refresh tests."""

    def __init__(self) -> None:
        super().__init__()
        self.interval: str | None = None
        self.callback = None

    def fragment(self, *, run_every: str):
        self.interval = run_every

        def decorate(callback):
            self.callback = callback
            return callback

        return decorate


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


def test_real_active_capture_shows_lag_without_inventing_count_or_observation_time(
        tmp_path: Path) -> None:
    """Read the producer's actual active manifest without opening raw segments."""
    from pixguard_ai.application.capture_events import CaptureRequest
    from pixguard_ai.infrastructure.kafka.capture_storage import CaptureStorage
    from pixguard_ai.infrastructure.local.streaming_results import load_latest_streaming_results

    storage = CaptureStorage(tmp_path)
    try:
        storage.reserve(CaptureRequest("capture-active", "cluster-1", "topic", 0, 5, None, tmp_path))
        storage.fix_end(20)
        storage.confirmed(11)
    finally:
        storage.close()
    state = load_latest_streaming_results(tmp_path)
    assert not state.error
    assert "count" not in state.capture
    dashboard = Dashboard()
    render_streaming_results(dashboard, state)
    text = " ".join(dashboard.lines + dashboard.metrics)
    assert "Coordenadas capturadas: Não disponível" in text
    assert "Lag físico de offsets: 8" in text
    assert "Próximo offset durável: 12" in text
    assert "fim exclusivo: 20" in text
    assert f"Início da captura UTC: {state.capture['started_at']}" in text
    assert "Instante de atualização da posição: não disponível" in text


def test_completed_stages_display_start_and_finish_timestamps() -> None:
    dashboard = Dashboard()
    started = "2026-11-20T10:00:00+00:00"
    finished = "2026-11-20T10:00:10+00:00"
    state = StreamingSnapshot(
        {"source_batch_id": "batch", "status": "completed", "total": 0,
         "confirmed": 0, "uncertain": 0, "not_sent": 0,
         "started_at": started, "finished_at": finished},
        {"capture_id": "capture", "status": "completed", "topic": "topic", "partition": 0,
         "start": 0, "end": 10, "count": 0, "started_at": started, "finished_at": finished},
        None, "Manifestos disponíveis.",
    )
    render_streaming_results(dashboard, state)
    text = " ".join(dashboard.lines)
    for stage in ("publicação", "captura"):
        assert f"Início da {stage} UTC: {started}" in text
        assert f"Conclusão da {stage} UTC: {finished}" in text


def test_streaming_fragment_reloads_manifests_on_each_periodic_refresh(tmp_path: Path) -> None:
    from pixguard_ai.interfaces.dashboard import render_streaming_fragment

    manifest_path = tmp_path / "publications" / "batch-1" / "manifest.json"
    first = {
        "manifest_version": 1, "source_batch_id": "batch-1", "status": "completed",
        "confirmed": 1, "uncertain": 0, "not_sent": 0, "total": 1,
        "started_at": "2026-11-20T10:00:00+00:00",
        "finished_at": "2026-11-20T10:00:01+00:00",
    }
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(first), encoding="utf-8")
    dashboard = FragmentDashboard()

    render_streaming_fragment(dashboard, tmp_path)
    assert dashboard.interval == "2s"
    assert "batch-1" in " ".join(dashboard.lines)

    second = {**first, "source_batch_id": "batch-2"}
    other_path = tmp_path / "publications" / "batch-2" / "manifest.json"
    other_path.parent.mkdir(parents=True)
    other_path.write_text(json.dumps(second), encoding="utf-8")
    manifest_path.unlink()
    assert dashboard.callback is not None
    dashboard.callback()

    assert "batch-2" in " ".join(dashboard.lines)
