"""Read validated Spark run manifests and their bounded dashboard summaries."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path
import re
from typing import Any


MAX_MANIFEST_BYTES = 65_536
MAX_SUMMARY_BYTES = 262_144
MAX_RUN_DIRECTORIES = 100_000
MAX_TOP_GROUPS = 20
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")


@dataclass(frozen=True)
class PipelineSnapshot:
    """Represent the newest valid completed pipeline run for display."""

    manifest: dict[str, Any] | None
    top_groups: tuple[dict[str, Any], ...]
    message: str
    error: bool = False
    newer_failure: str | None = None


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate JSON keys in persisted metadata."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _timestamp(value: object) -> datetime:
    """Parse an ISO timestamp that explicitly represents UTC."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must use UTC")
    return parsed


def _read_json(path: Path, limit: int) -> object:
    """Read a small JSON metadata file without allowing unbounded allocation."""
    with path.open("rb") as stream:
        payload = stream.read(limit + 1)
    if len(payload) > limit:
        raise ValueError("metadata exceeds the read limit")
    return json.loads(payload, object_pairs_hook=_unique_object)


def _safe_path(root: Path, relative: object) -> Path:
    """Resolve an artifact path while keeping symlinks and traversal in the data root."""
    if not isinstance(relative, str) or not relative:
        raise ValueError("artifact path is missing")
    candidate = (root / relative).resolve(strict=True)
    if not candidate.is_relative_to(root):
        raise ValueError("artifact path escapes the data root")
    return candidate


def _nonnegative_integer(value: object) -> bool:
    """Accept plain nonnegative JSON integers, excluding booleans."""
    return type(value) is int and value >= 0


def _validate_summary(value: object, gold_count: int) -> tuple[dict[str, Any], ...]:
    """Validate top-N rows and exact decimal amounts before chart rendering."""
    if not isinstance(value, dict) or value.get("label_is_synthetic") is not True:
        raise ValueError("summary is incomplete or not marked synthetic")
    if not _nonnegative_integer(value.get("gold_group_count")):
        raise ValueError("summary Gold count is invalid")
    if value["gold_group_count"] != gold_count:
        raise ValueError("summary and manifest Gold counts differ")
    rows = value.get("top_groups")
    if not isinstance(rows, list) or len(rows) > MAX_TOP_GROUPS or len(rows) > gold_count:
        raise ValueError("summary top-N exceeds its contract")
    validated: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("summary row is invalid")
        if not all(isinstance(row.get(key), str) and row[key] for key in
                   ("event_day_utc", "merchant_id", "channel", "status")):
            raise ValueError("summary dimension is invalid")
        if not _nonnegative_integer(row.get("event_count")) or row["event_count"] < 1:
            raise ValueError("summary event count is invalid")
        amount = row.get("amount_total_brl")
        if not isinstance(amount, str):
            raise ValueError("summary amount must preserve decimal text")
        try:
            parsed_amount = Decimal(amount)
        except InvalidOperation as error:
            raise ValueError("summary amount is invalid") from error
        if not parsed_amount.is_finite():
            raise ValueError("summary amount is not finite")
        validated.append({**row, "amount_total_brl": format(parsed_amount, ".2f")})
    return tuple(validated)


def _validate_completed(root: Path, manifest_path: Path, value: object) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], datetime]:
    """Validate completed-run metadata, artifacts, and reconciliation counts."""
    if not isinstance(value, dict):
        raise ValueError("manifest is not an object")
    required = {"manifest_version", "run_id", "status", "schema_version", "pipeline_version",
                "label_is_synthetic", "input", "counts", "reconciliation", "outputs",
                "dashboard", "started_at", "finished_at", "elapsed_seconds"}
    if not required <= value.keys():
        raise ValueError("completed manifest is incomplete")
    run_id = value["run_id"]
    if (type(value["manifest_version"]) is not int or value["manifest_version"] != 1
            or value["status"] != "completed" or value["label_is_synthetic"] is not True
            or not isinstance(run_id, str) or _RUN_ID.fullmatch(run_id) is None
            or manifest_path.parent.name != run_id):
        raise ValueError("completed manifest identity is invalid")
    if not isinstance(value["schema_version"], str) or not value["schema_version"]:
        raise ValueError("schema version is invalid")
    if not isinstance(value["pipeline_version"], str) or not value["pipeline_version"]:
        raise ValueError("pipeline version is invalid")
    started_at = _timestamp(value["started_at"])
    finished_at = _timestamp(value["finished_at"])
    if finished_at < started_at:
        raise ValueError("run timestamps are inconsistent")
    elapsed = value["elapsed_seconds"]
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
        raise ValueError("elapsed time is invalid")

    input_data = value["input"]
    counts = value["counts"]
    if not isinstance(input_data, dict) or not isinstance(counts, dict):
        raise ValueError("input or counts are invalid")
    if not all(_nonnegative_integer(counts.get(key)) for key in
               ("bronze", "silver_unique", "duplicates_removed", "quarantine", "gold")):
        raise ValueError("layer counts are invalid")
    if not _nonnegative_integer(input_data.get("rows")) or input_data["rows"] != counts["bronze"]:
        raise ValueError("input and Bronze counts differ")
    if input_data["rows"] != (counts["silver_unique"] + counts["duplicates_removed"] + counts["quarantine"]):
        raise ValueError("manifest counts do not reconcile")
    if not isinstance(input_data.get("sha256"), str) or _SHA256.fullmatch(input_data["sha256"]) is None:
        raise ValueError("input checksum is invalid")
    _safe_path(root, input_data.get("path"))

    reconciliation = value["reconciliation"]
    if not isinstance(reconciliation, dict) or reconciliation.get("passed") is not True:
        raise ValueError("manifest reconciliation did not pass")
    outputs = value["outputs"]
    if not isinstance(outputs, dict) or not {"bronze", "silver", "quarantine", "gold"} <= outputs.keys():
        raise ValueError("layer outputs are incomplete")
    output_paths = {name: _safe_path(root, relative) for name, relative in outputs.items()}
    gold_path = output_paths["gold"]
    if not gold_path.is_dir() or not any(path.is_file() and path.suffix == ".parquet"
                                        and path.resolve().is_relative_to(root)
                                        for path in gold_path.rglob("*.parquet")):
        raise ValueError("Gold Parquet output is unavailable")

    dashboard = value["dashboard"]
    if not isinstance(dashboard, dict) or dashboard.get("gold_path") != outputs["gold"]:
        raise ValueError("dashboard Gold reference does not match outputs")
    summary_path = _safe_path(root, dashboard.get("summary_path"))
    summary = _read_json(summary_path, MAX_SUMMARY_BYTES)
    groups = _validate_summary(summary, counts["gold"])
    embedded = dashboard.get("summary")
    if (not isinstance(embedded, dict) or embedded.get("gold_rows") != counts["gold"]
            or embedded.get("top_groups") != summary["top_groups"]):
        raise ValueError("summary and manifest dashboard values differ")
    return value, groups, finished_at


def _latest_failure(directories: list[Path]) -> tuple[datetime | None, str | None]:
    """Find a newer recorded failed run for a warning beside the last good result."""
    newest: tuple[datetime, str] | None = None
    for run_dir in directories:
        path = run_dir / "manifest.json"
        try:
            value = _read_json(path, MAX_MANIFEST_BYTES)
            if not isinstance(value, dict) or value.get("status") != "failed":
                continue
            if value.get("run_id") != run_dir.name or not _RUN_ID.fullmatch(run_dir.name):
                continue
            timestamp = value.get("failed_at", value.get("finished_at", value.get("started_at")))
            if timestamp is None:
                timestamp = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
            stamp = _timestamp(timestamp)
            if newest is None or stamp > newest[0]:
                newest = (stamp, run_dir.name)
        except (OSError, ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
            continue
    return newest if newest else (None, None)


def load_latest_pipeline_results(data_root: Path) -> PipelineSnapshot:
    """Select and validate the newest completed run without reading Parquet rows."""
    try:
        root = data_root.resolve(strict=True)
        runs = root / "runs"
        if not runs.exists():
            return PipelineSnapshot(None, (), "Nenhuma execução Spark disponível.")
        if not runs.is_dir() or not runs.resolve(strict=True).is_relative_to(root):
            raise ValueError("runs directory is outside the data root")
        directories = []
        for path in runs.iterdir():
            if path.is_dir() and not path.is_symlink():
                directories.append(path)
                if len(directories) > MAX_RUN_DIRECTORIES:
                    raise ValueError("too many run directories")
    except FileNotFoundError:
        return PipelineSnapshot(None, (), "Nenhuma execução Spark disponível.")
    except (OSError, ValueError, RuntimeError):
        return PipelineSnapshot(None, (), "Diretório de execuções inválido ou indisponível.", True)

    candidates: list[tuple[datetime, dict[str, Any], tuple[dict[str, Any], ...]]] = []
    invalid_count = 0
    for run_dir in directories:
        manifest_path = run_dir / "manifest.json"
        try:
            if not manifest_path.is_file() or manifest_path.is_symlink():
                continue
            value = _read_json(manifest_path, MAX_MANIFEST_BYTES)
            if isinstance(value, dict) and value.get("status") != "completed":
                continue
            manifest, groups, finished_at = _validate_completed(root, manifest_path, value)
            candidates.append((finished_at, manifest, groups))
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError,
                OverflowError, InvalidOperation, RuntimeError):
            invalid_count += 1
    if not candidates:
        if invalid_count:
            return PipelineSnapshot(None, (), "Nenhum manifesto concluído válido foi encontrado.", True)
        failed_at, failed_id = _latest_failure(directories)
        if failed_id is not None:
            return PipelineSnapshot(None, (), f"A execução {failed_id} falhou; ainda não há resultado concluído.", True)
        return PipelineSnapshot(None, (), "Nenhuma execução Spark concluída disponível.")

    _, manifest, groups = max(candidates, key=lambda item: item[0])
    newer_failure = None
    failed_at, failed_id = _latest_failure(directories)
    if failed_id is not None and failed_at is not None and failed_at > _timestamp(manifest["finished_at"]):
        newer_failure = failed_id
    message = "Resultados da última execução Spark concluída."
    if invalid_count:
        message += f" {invalid_count} manifesto(s) inválido(s) foram ignorados."
    return PipelineSnapshot(manifest, groups, message, bool(invalid_count), newer_failure)
