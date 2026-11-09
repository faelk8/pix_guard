"""Verify truthful dashboard snapshots and Streamlit rendering in containers."""

from copy import deepcopy
import json
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from pixguard_ai.application.generate_events import GenerationConfig, GenerationManifest, generate_events
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.infrastructure.synthetic.jsonl import JsonlEventWriter
from pixguard_ai.interfaces import dashboard


@pytest.fixture
def persisted(tmp_path: Path) -> tuple[Path, GenerationManifest]:
    """Produce a genuine small run using the application-owned contract."""
    output = tmp_path / "events.jsonl"
    manifest = generate_events(GenerationConfig(count=5), SyntheticEventSource(), JsonlEventWriter(output))
    return tmp_path / "events.manifest.json", manifest


def write_manifest(path: Path, manifest: object) -> None:
    """Replace isolated test metadata without touching the shared dataset."""
    path.write_text(json.dumps(manifest), encoding="utf-8")


def test_real_persisted_completed_snapshot(persisted: tuple[Path, GenerationManifest]) -> None:
    """Read the actual writer's manifest and do not scan JSONL records."""
    path, manifest = persisted
    snapshot = dashboard.load_snapshot(path)
    assert snapshot.manifest == manifest
    assert snapshot.data_available and not snapshot.error
    assert dashboard.format_count(100_000) == "100.000"
    assert dashboard.format_volume(1_048_576) == "1.00 MiB (1.048.576 bytes)"


@pytest.mark.parametrize("status", ["running", "failed", "interrupted"])
def test_current_run_uses_partial_not_previous_output(
    persisted: tuple[Path, GenerationManifest], status: str,
) -> None:
    """An existing old published file cannot stand in for a missing current run."""
    path, manifest = persisted
    manifest.update(status=status, finished_at=None if status == "running" else manifest["finished_at"])
    write_manifest(path, manifest)
    assert dashboard.load_snapshot(path).error
    Path(manifest["partial_path"]).write_bytes(b"x" * manifest["bytes_written"])
    assert dashboard.load_snapshot(path).data_available


@pytest.mark.parametrize("payload", [b"{", b"[]", b"null", b"\xff", b'{"status":1,"status":2}', b" " * 65_537])
def test_invalid_json_is_explicit(tmp_path: Path, payload: bytes) -> None:
    """Malformed, ambiguous and oversized snapshots have no fabricated metrics."""
    path = tmp_path / "events.manifest.json"
    path.write_bytes(payload)
    snapshot = dashboard.load_snapshot(path)
    assert snapshot.error and snapshot.manifest is None


@pytest.mark.parametrize(
    ("field", "value"),
    [("count_written", True), ("count_requested", 0), ("count_written", 6),
     ("progress", 0.1), ("progress", float("nan")), ("elapsed_seconds", float("inf")),
     ("label_is_synthetic", False), ("manifest_version", 2), ("status", "unknown"),
     ("updated_at", "yesterday"), ("started_at", "2026-01-01T00:00:00"),
     ("scenario_counts", {"normal": 9}), ("error", []), ("sha256", "bad")],
)
def test_invalid_contract_has_no_metrics(
    persisted: tuple[Path, GenerationManifest], field: str, value: object,
) -> None:
    """Reject untrusted metadata before it reaches Streamlit's progress widget."""
    path, original = persisted
    manifest = deepcopy(original)
    manifest[field] = value
    write_manifest(path, manifest)
    snapshot = dashboard.load_snapshot(path)
    assert snapshot.error and snapshot.manifest is None


def test_missing_manifest_and_file(persisted: tuple[Path, GenerationManifest]) -> None:
    """Distinguish absence of metadata from missing published data."""
    path, manifest = persisted
    Path(manifest["output_path"]).unlink()
    assert dashboard.load_snapshot(path).error
    path.unlink()
    snapshot = dashboard.load_snapshot(path)
    assert snapshot.manifest is None and not snapshot.error


def test_short_file_and_outside_path(persisted: tuple[Path, GenerationManifest], tmp_path: Path) -> None:
    """Check availability without accepting a path outside the dataset directory."""
    path, manifest = persisted
    Path(manifest["output_path"]).write_bytes(b"short")
    assert "menor" in dashboard.load_snapshot(path).message
    manifest["output_path"] = str(tmp_path.parent / "outside.jsonl")
    write_manifest(path, manifest)
    assert "caminho inválido" in dashboard.load_snapshot(path).message


def test_unreadable_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A filesystem error is rendered as an unavailable snapshot."""
    def fail_open(*args: object, **kwargs: object) -> None:
        """Simulate denied access independently of container user permissions."""
        raise PermissionError

    monkeypatch.setattr(Path, "open", fail_open)
    assert dashboard.load_snapshot(tmp_path / "events.manifest.json").error


@pytest.mark.parametrize("status", ["completed", "running", "failed", "interrupted"])
def test_render_status_and_real_metrics(
    persisted: tuple[Path, GenerationManifest], monkeypatch: pytest.MonkeyPatch, status: str,
) -> None:
    """Exercise the actual Streamlit page for all lifecycle states."""
    path, manifest = persisted
    manifest.update(status=status, finished_at=None if status == "running" else manifest["finished_at"])
    if status != "completed":
        manifest.update(count_written=2, progress=0.4, scenario_counts={"normal": 2}, bytes_written=10)
        Path(manifest["partial_path"]).write_bytes(b"x" * 10)
    if status in ("failed", "interrupted"):
        manifest["error"] = "ValueError" if status == "failed" else "KeyboardInterrupt"
    write_manifest(path, manifest)
    monkeypatch.setenv("PIXGUARD_DATA_DIR", str(path.parent))
    app = AppTest.from_file(str(Path(dashboard.__file__))).run()
    assert not app.exception
    assert any("100% SINTÉTICOS" in warning.value for warning in app.warning)
    assert app.subheader[0].value == f"Estado da geração: {dashboard.STATUS_LABELS[status]}"
    assert app.metric[0].value == str(manifest["count_written"])
    assert app.metric[1].value == "5"
    assert app.metric[2].value == dashboard.format_volume(manifest["bytes_written"])
    texts = " ".join(item.value for item in app.markdown)
    assert manifest["updated_at"] in texts
    assert f"Erro registrado: {manifest['error'] or 'Nenhum'}" in texts
    if status == "completed":
        assert len(app.success) == 1
    elif status in ("failed", "interrupted"):
        assert len(app.error) == 1


@pytest.mark.parametrize("invalid", [False, True])
def test_render_empty_or_invalid_has_banner_without_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invalid: bool,
) -> None:
    """The synthetic notice remains visible even before usable metadata exists."""
    if invalid:
        (tmp_path / "events.manifest.json").write_text("{", encoding="utf-8")
    monkeypatch.setenv("PIXGUARD_DATA_DIR", str(tmp_path))
    app = AppTest.from_file(str(Path(dashboard.__file__))).run()
    assert not app.exception
    assert "100% SINTÉTICOS" in app.warning[0].value
    assert not app.metric
    assert bool(app.error) == invalid
