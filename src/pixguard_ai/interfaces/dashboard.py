"""Display persisted generation snapshots without loading the event dataset."""

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import math
import os
from pathlib import Path
import re
from types import ModuleType
from typing import cast

from pixguard_ai.application.generate_events import GenerationManifest


MAX_MANIFEST_BYTES = 65_536
STATUS_LABELS = {
    "running": "Em execução",
    "completed": "Concluído",
    "failed": "Falhou",
    "interrupted": "Interrompido",
}


@dataclass(frozen=True)
class DashboardSnapshot:
    """Keep unavailable snapshots distinct from an observed zero count."""

    manifest: GenerationManifest | None
    message: str
    error: bool = False
    data_available: bool = False


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject ambiguous duplicate JSON keys rather than silently replacing them."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Campo duplicado no manifesto.")
        result[key] = value
    return result


def _validate_manifest(value: object) -> GenerationManifest:
    """Validate the versioned producer contract before exposing metrics."""
    if not isinstance(value, dict) or not GenerationManifest.__required_keys__ <= value.keys():
        raise ValueError("Contrato do manifesto incompleto.")
    if type(value["manifest_version"]) is not int or value["manifest_version"] != 1:
        raise ValueError("Versão do manifesto não suportada.")
    if value["label_is_synthetic"] is not True:
        raise ValueError("O manifesto não identifica dados sintéticos.")
    if not isinstance(value["status"], str) or value["status"] not in STATUS_LABELS:
        raise ValueError("Status do manifesto inválido.")
    for key in ("count_requested", "count_written", "bytes_written", "seed"):
        if type(value[key]) is not int or value[key] < 0:
            raise ValueError("Contador do manifesto inválido.")
    requested, written = value["count_requested"], value["count_written"]
    if requested < 1 or written > requested:
        raise ValueError("Contadores do manifesto inconsistentes.")
    for key in ("progress", "elapsed_seconds"):
        if type(value[key]) not in (float, int) or not math.isfinite(value[key]) or value[key] < 0:
            raise ValueError("Métrica do manifesto inválida.")
    if not math.isclose(value["progress"], written / requested, abs_tol=1e-9):
        raise ValueError("Progresso diverge dos contadores persistidos.")
    for key in ("schema_version", "output_path", "partial_path", "sha256"):
        if not isinstance(value[key], str) or not value[key]:
            raise ValueError("Campo textual do manifesto inválido.")
    if re.fullmatch(r"[0-9a-f]{64}", value["sha256"]) is None:
        raise ValueError("Checksum do manifesto inválido.")
    for key in ("started_at", "updated_at", "finished_at"):
        stamp = value[key]
        if key == "finished_at" and stamp is None and value["status"] == "running":
            continue
        if not isinstance(stamp, str) or datetime.fromisoformat(stamp).utcoffset() != timedelta(0):
            raise ValueError("Timestamp do manifesto inválido.")
    counts = value["scenario_counts"]
    if not isinstance(counts, dict) or any(
        not isinstance(key, str) or type(count) is not int or count < 0
        for key, count in counts.items()
    ) or sum(counts.values()) != written:
        raise ValueError("Contagem dos cenários inconsistente.")
    if value["error"] is not None and not isinstance(value["error"], str):
        raise ValueError("Erro do manifesto inválido.")
    if value["status"] == "completed" and written != requested:
        raise ValueError("Lote concluído com contagem incompleta.")
    return cast(GenerationManifest, value)


def load_snapshot(path: Path) -> DashboardSnapshot:
    """Read a bounded JSON snapshot and stat only the current run's local file."""
    try:
        with path.open("rb") as stream:
            payload = stream.read(MAX_MANIFEST_BYTES + 1)
        if len(payload) > MAX_MANIFEST_BYTES:
            raise ValueError("Manifesto excede o limite de leitura.")
        manifest = _validate_manifest(json.loads(payload, object_pairs_hook=_unique_object))
    except FileNotFoundError:
        return DashboardSnapshot(None, "Nenhuma geração disponível: manifesto ainda não existe.")
    except (ValueError, UnicodeError, RecursionError, OverflowError):
        return DashboardSnapshot(None, "Manifesto inválido ou incompatível. Verifique a geração.", True)
    except OSError:
        return DashboardSnapshot(None, "Não foi possível ler o manifesto persistido.", True)

    key = "output_path" if manifest["status"] == "completed" else "partial_path"
    try:
        data_path = Path(manifest[key]).resolve()
        if not data_path.is_relative_to(path.parent.resolve()):
            raise ValueError("Arquivo fora do volume de dados.")
        if not data_path.is_file():
            return DashboardSnapshot(manifest, "Arquivo de dados desta execução não encontrado.", True)
        if data_path.stat().st_size < manifest["bytes_written"]:
            return DashboardSnapshot(manifest, "Arquivo menor que o volume registrado no manifesto.", True)
    except (OSError, ValueError, RuntimeError):
        return DashboardSnapshot(manifest, "Arquivo de dados indisponível ou caminho inválido.", True)
    return DashboardSnapshot(manifest, "Arquivo da execução disponível no volume compartilhado.", data_available=True)


def format_count(value: int) -> str:
    """Format observed integers with Brazilian grouping."""
    return f"{value:,}".replace(",", ".")


def format_volume(value: int) -> str:
    """Keep exact byte counts beside the human-readable binary volume."""
    return f"{value / (1024 * 1024):.2f} MiB ({format_count(value)} bytes)"


def render_snapshot(st: ModuleType, snapshot: DashboardSnapshot) -> None:
    """Render a snapshot; absent data never becomes an invented zero metric."""
    if snapshot.manifest is None:
        (st.error if snapshot.error else st.info)(snapshot.message)
        return
    manifest = snapshot.manifest
    status = manifest["status"]
    st.subheader(f"Estado da geração: {STATUS_LABELS[status]}")
    if status == "running":
        st.info("Geração em andamento — exibindo o último snapshot persistido.")
        st.caption("Um snapshot em execução não comprova que o processo continua ativo; confira a última atualização.")
    elif status == "completed":
        if snapshot.data_available:
            st.success("Geração concluída. Total final registrado abaixo.")
    else:
        st.error("A geração falhou." if status == "failed" else "A geração foi interrompida.")
        st.warning("O lote atual está incompleto. Um lote anterior publicado não pertence a esta execução.")
    (st.error if snapshot.error else st.caption)(snapshot.message)
    written, requested = manifest["count_written"], manifest["count_requested"]
    st.progress(written / requested, text=f"{format_count(written)} / {format_count(requested)} eventos persistidos")
    st.metric("Eventos produzidos", format_count(written))
    st.metric("Eventos solicitados", format_count(requested))
    st.metric("Volume gravado no snapshot", format_volume(manifest["bytes_written"]))
    st.write(f"Última atualização (UTC): {manifest['updated_at']}")
    st.write(f"Início (UTC): {manifest['started_at']}")
    st.write(f"Fim (UTC): {manifest['finished_at'] or 'Ainda não registrado'}")
    st.write(f"Duração registrada: {manifest['elapsed_seconds']:.2f} s")
    st.write(f"Status final: {STATUS_LABELS[status] if status != 'running' else 'Pendente'}")
    st.write(f"Erro registrado: {manifest['error'] or 'Nenhum'}")
    st.caption("Contadores e volume vêm do manifesto; integridade e checksum do JSONL não são recalculados nesta tela.")


def main() -> None:
    """Start Streamlit with a permanent synthetic-data notice and live snapshots."""
    import streamlit as st

    st.set_page_config(page_title="PixGuard AI — geração sintética", page_icon="🧪")
    st.title("PixGuard AI")
    st.warning("DADOS 100% SINTÉTICOS — eventos fictícios para estudo, sem transações ou pessoas reais.")
    st.caption("Progresso do gerador local • lote padrão de 100.000 eventos")
    path = Path(os.environ.get("PIXGUARD_DATA_DIR", "/data/pixguard")) / "events.manifest.json"

    @st.fragment(run_every="2s")
    def refresh() -> None:
        """Refresh only the persisted snapshot, never the complete JSONL file."""
        render_snapshot(st, load_snapshot(path))

    refresh()


if __name__ == "__main__":
    main()
