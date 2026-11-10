"""Display persisted generation snapshots without loading the event dataset."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import re
from types import ModuleType
from typing import cast

from pixguard_ai.application.generate_events import GenerationManifest
from pixguard_ai.infrastructure.local.pipeline_results import (
    PipelineSnapshot,
    load_latest_pipeline_results,
)
from pixguard_ai.infrastructure.local.analysis_results import (
    AnalysisSnapshot,
    load_latest_analysis_results,
)


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

    st.set_page_config(page_title="PixGuard AI — FASE 3", page_icon="🧪")
    st.title("PixGuard AI")
    st.warning("DADOS 100% SINTÉTICOS — eventos fictícios para estudo, sem transações ou pessoas reais.")
    st.caption("Progresso do gerador local • lote padrão de 100.000 eventos")
    path = Path(os.environ.get("PIXGUARD_DATA_DIR", "/data/pixguard")) / "events.manifest.json"

    @st.fragment(run_every="2s")
    def refresh() -> None:
        """Refresh only the persisted snapshot, never the complete JSONL file."""
        render_snapshot(st, load_snapshot(path))

    refresh()
    st.divider()
    st.header("Processamento offline — FASE 3")
    render_pipeline_results(st, load_latest_pipeline_results(Path(os.environ.get(
        "PIXGUARD_DATA_DIR", "/data/pixguard"))))
    st.divider()
    st.header("Avaliação da regra — FASE 4")
    render_analysis_results(st, load_latest_analysis_results(Path(os.environ.get(
        "PIXGUARD_DATA_DIR", "/data/pixguard"))))


def render_pipeline_results(st: ModuleType, snapshot: PipelineSnapshot) -> None:
    """Render completed Spark metrics and the bounded Gold summary without loading Spark."""
    st.caption("Esta etapa processa o JSONL sintético da FASE 1. Não consome Kafka.")
    if snapshot.manifest is None:
        (st.error if snapshot.error else st.info)(snapshot.message)
        return

    manifest = snapshot.manifest
    if snapshot.error:
        st.warning(snapshot.message)
    else:
        st.success(snapshot.message)
    if snapshot.newer_failure:
        st.warning(f"A execução mais recente {snapshot.newer_failure} falhou; os indicadores abaixo vêm da última execução concluída.")
    st.write(f"Execução: `{manifest['run_id']}` • início UTC: {manifest['started_at']} • fim UTC: {manifest['finished_at']}")
    counts = manifest["counts"]
    columns = st.columns(5)
    labels = (("Entrada / Bronze", counts["bronze"]), ("Silver válidos", counts["silver_unique"]),
              ("Duplicatas removidas", counts["duplicates_removed"]), ("Quarentena", counts["quarantine"]),
              ("Grupos Gold", counts["gold"]))
    for column, (label, value) in zip(columns, labels):
        column.metric(label, format_count(value))
    st.caption("Reconciliação: entrada = Silver válidos + duplicatas removidas + quarentena. Valores do manifesto concluído.")
    st.subheader("Resumo Gold: maiores grupos por volume de eventos")
    if snapshot.top_groups:
        table = [{"Dia UTC": row["event_day_utc"], "Comerciante sintético": row["merchant_id"],
                  "Canal": row["channel"], "Status": row["status"],
                  "Eventos": format_count(row["event_count"]),
                  "Total BRL": row["amount_total_brl"]} for row in snapshot.top_groups]
        st.dataframe(table, use_container_width=True, hide_index=True)
        st.bar_chart({"Eventos por grupo (top 20)": [row["event_count"] for row in snapshot.top_groups]},
                     height=260)
        st.caption("A tabela e o gráfico usam somente a amostra Gold resumida (até 20 grupos); BRL permanece como texto decimal exato.")
    else:
        st.info("A execução concluiu sem grupos Gold para resumir.")
    st.warning("Simulação didática: os rótulos e agregados sintéticos não medem desempenho antifraude real.")


def _format_metric(metrics: dict[str, object], name: str) -> str:
    """Format a validated metric or explain why it has no defined value."""
    value = metrics[name]
    if not value["defined"]:
        reasons = {
            "no_alerts": "indefinida: nenhuma previsão de alerta",
            "no_positive_targets": "indefinida: sem exemplos positivos",
            "no_positive_targets_or_alerts": "indefinida: sem positivos e sem alertas",
            "no_negative_targets": "indefinida: sem exemplos negativos",
            "no_evaluable_rows": "indefinida: população vazia",
            "requires_both_classes": "indefinida: requer exemplos positivos e negativos",
            "not_applicable_fixed_decision": "não aplicável a uma decisão fixa",
            "zero_positive_support": "indefinida: sem exemplos positivos",
            "zero_negative_support": "indefinida: sem exemplos negativos",
            "empty_population": "indefinida: população vazia",
        }
        return reasons.get(value["reason"], f"indefinida: {value['reason']}")
    return f"{value['value']:.2%}"


def render_analysis_results(st: ModuleType, snapshot: AnalysisSnapshot) -> None:
    """Render a verified fixed-rule evaluation; the dashboard never computes scores."""
    if snapshot.manifest is None:
        (st.error if snapshot.error else st.info)(snapshot.message)
        st.caption("A análise é calculada separadamente em Spark e publicada como artefato imutável.")
        return

    manifest = snapshot.manifest
    metrics = snapshot.metrics["metrics"]
    baseline = snapshot.metrics["always_negative"]
    counts = manifest["counts"]
    rule = manifest["rule"]
    if snapshot.error:
        st.warning(snapshot.message)
    else:
        st.success(snapshot.message)
    if snapshot.newer_failure:
        st.warning(f"A análise mais recente {snapshot.newer_failure} falhou; os indicadores abaixo vêm da última análise concluída válida.")
    st.write(f"Execução: `{manifest['analysis_run_id']}` • origem: `{manifest['source_run_id']}` • fim UTC: {manifest['finished_at']}")
    threshold = f"{Decimal(rule['threshold_brl']):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    st.write(f"Regra fixa `{rule['rule_id']}` v{rule['rule_version']}: valor ≥ R$ {threshold} gera alerta.")
    cols = st.columns(5)
    for column, (label, name) in zip(cols, (("Eventos avaliados", "N"), ("Alertas", "alerts"),
                                            ("Positivos sintéticos", "P"), ("Negativos sintéticos", "Nneg"),
                                            ("Tempo Spark", None))):
        value = f"{manifest['elapsed_seconds']:.2f} s" if name is None else format_count(counts[name])
        column.metric(label, value)
    st.caption("Reconciliação: Silver = previsões = avaliação = TP + FP + FN + TN. Rótulos são usados somente após persistir previsões.")

    st.subheader("Matriz de confusão")
    matrix = {
        "Resultado": ["Alerta previsto", "Sem alerta previsto"],
        "Positivo sintético": [format_count(counts["TP"]), format_count(counts["FN"])],
        "Negativo sintético": [format_count(counts["FP"]), format_count(counts["TN"])],
    }
    st.dataframe(matrix, use_container_width=True, hide_index=True)
    st.caption("TP: positivo corretamente alertado • FP: negativo alertado • FN: positivo sem alerta • TN: negativo sem alerta.")

    st.subheader("Métricas da regra")
    metric_names = (("Precisão", "precision"), ("Revocação", "recall"), ("F1", "f1"),
                    ("Taxa de falso positivo", "fpr"), ("PR-AUC", "pr_auc"),
                    ("Acurácia (secundária)", "accuracy"), ("Prevalência", "prevalence"),
                    ("Taxa de alertas", "alert_rate"))
    cols = st.columns(4)
    for column, (label, name) in zip(cols * 2, metric_names):
        column.metric(label, _format_metric(metrics, name))
    baseline_metrics = baseline["metrics"]
    st.write("**Baseline sempre negativo**")
    st.caption(f"Acurácia { _format_metric(baseline_metrics, 'accuracy') } • precisão { _format_metric(baseline_metrics, 'precision') } • revocação { _format_metric(baseline_metrics, 'recall') } • alertas {format_count(baseline['counts']['TP'] + baseline['counts']['FP'])}")

    st.subheader("Amostra de explicações")
    examples = snapshot.summary["examples"]
    if examples:
        table = [{"ID sintético": item["event_id"], "Valor (BRL)": item["amount_brl"],
                  "Alerta": "Sim" if item["predicted_suspicious"] else "Não",
                  "Motivo": item["reason"], "Código": item["reason_code"]}
                 for item in examples]
        st.dataframe(table, use_container_width=True, hide_index=True)
    else:
        st.info("Esta avaliação válida não contém exemplos para exibir.")
    st.warning("Dados e rótulos 100% sintéticos. Esta avaliação compara uma regra fixa com os rótulos artificiais do simulador; não comprova detecção de fraude, não mede fraude real nem demonstra generalização. Nenhum ajuste de limiar é feito nesta tela.")


if __name__ == "__main__":
    main()
