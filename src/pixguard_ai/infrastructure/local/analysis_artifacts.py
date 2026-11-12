"""Bounded metadata, safe local paths and immutable analysis publication."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Iterator

from pixguard_ai.domain.analysis import RuleConfig
from pixguard_ai.domain.pipeline import validate_run_id

MAX_JSON_BYTES = 262_144
PROTOCOL_ID = "fixed_simulator_conformance_v1"
WARNING = ("Regra baseada nas faixas artificiais do gerador; não comprova detecção "
           "de fraude. Avaliação conhecida, não cega e sem evidência de generalização.")


def utc_now() -> str:
    """Return an explicit UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def canonical_bytes(value: object) -> bytes:
    """Encode finite canonical JSON for reproducible configuration hashes."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(path: Path) -> str:
    """Hash a file in bounded blocks."""
    result = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def safe_path(root: Path, relative: str, *, exists: bool = True) -> Path:
    """Reject absolute paths, traversal and every symlink component."""
    if not isinstance(relative, str) or not relative:
        raise ValueError("missing artifact path")
    part = Path(relative)
    if part.is_absolute() or ".." in part.parts or "." == relative:
        raise ValueError("unsafe artifact path")
    current = root
    if root.is_symlink():
        raise ValueError("symlink root")
    for component in part.parts:
        current = current / component
        if current.is_symlink():
            raise ValueError("symlink artifact")
    if not current.resolve(strict=exists).is_relative_to(root.resolve(strict=True)):
        raise ValueError("artifact escapes root")
    return current


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject ambiguous duplicate JSON keys."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def read_json(path: Path) -> dict[str, Any]:
    """Read bounded object metadata with strict JSON numbers and keys."""
    if path.is_symlink():
        raise ValueError("symlink metadata")
    with path.open("rb") as stream:
        payload = stream.read(MAX_JSON_BYTES + 1)
    if len(payload) > MAX_JSON_BYTES:
        raise ValueError("metadata exceeds limit")
    value = json.loads(payload, object_pairs_hook=_unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    if not isinstance(value, dict):
        raise ValueError("metadata must be an object")
    return value


def atomic_json(path: Path, value: object) -> None:
    """Flush JSON before a same-directory atomic rename."""
    payload = canonical_bytes(value)
    if len(payload) > MAX_JSON_BYTES:
        raise ValueError("metadata exceeds limit")
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def inventory(root: Path, directory: Path) -> list[dict[str, Any]]:
    """Inventory every data file; reject symlinks even in hidden Spark sidecars."""
    entries = []
    for path in sorted(directory.rglob("*")):
        safe_path(root, str(path.relative_to(root)))
        if path.is_file() and path.suffix == ".parquet":
            entries.append(file_record(root, path))
    if not entries:
        raise ValueError("Parquet inventory is empty")
    return entries


def file_record(root: Path, path: Path) -> dict[str, Any]:
    """Describe one file using a root-relative path and its actual bytes."""
    safe_path(root, str(path.relative_to(root)))
    return {"path": str(path.relative_to(root)), "sha256": digest(path),
            "size_bytes": path.stat().st_size}


def verify_records(root: Path, records: list[dict[str, Any]]) -> None:
    """Fail when any recorded file has moved, changed or become a symlink."""
    if not isinstance(records, list) or not records:
        raise ValueError("missing artifact inventory")
    seen = set()
    for record in records:
        path = safe_path(root, record["path"])
        if record["path"] in seen or file_record(root, path) != record:
            raise ValueError("artifact checksum/size mismatch or duplicate path")
        seen.add(record["path"])


def protocol(config: RuleConfig) -> dict[str, Any]:
    """Freeze the evaluation policy, not an optimized model or blind holdout."""
    return {"protocol_id": PROTOCOL_ID, "rule": config.to_dict(),
            "evaluation_kind": "simulator_conformance", "tuning": "none",
            "split": "all_eligible_silver", "blind": False,
            "generalization_claim": False, "positive_class": "is_suspicious=true",
            "score": "exact_integer_brl_cents_not_probability",
            "invalid_input": "fail_entire_run", "zero_denominator": "null",
            "pr_auc": {"evaluator": "BinaryClassificationEvaluator",
                       "metricName": "areaUnderPR", "numBins": 0,
                       "requires_both_classes": True, "weights": None},
            "metrics": ["precision", "recall", "f1", "fpr", "accuracy",
                        "prevalence", "alert_rate", "pr_auc"],
            "warning": WARNING}


class AnalysisArtifacts:
    """Local storage adapter; only the reserved new analysis directory is writable."""

    def __init__(self, root: Path) -> None:
        """Require an existing local data root."""
        if root.is_symlink():
            raise ValueError("symlink data root")
        self.root = root.resolve(strict=True)

    @contextmanager
    def reserve(self, run_id: str) -> Iterator[Path]:
        """Reserve a unique run and keep a failure manifest on ordinary interruption."""
        validate_run_id(run_id)
        runs = safe_path(self.root, "analysis-runs", exists=False)
        runs.mkdir(exist_ok=True)
        locks = safe_path(self.root, "analysis-locks", exists=False)
        locks.mkdir(exist_ok=True)
        lock = safe_path(self.root, f"analysis-locks/{run_id}.lock", exists=False)
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        created = False
        run = self.root / "analysis-runs" / run_id
        try:
            run = safe_path(self.root, f"analysis-runs/{run_id}", exists=False)
            run.mkdir(mode=0o750)
            created = True
            atomic_json(run / "pending.json", {"analysis_run_id": run_id,
                                             "status": "pending", "started_at": utc_now()})
            yield run
        except BaseException as error:
            if created and not (run / "manifest.json").exists():
                atomic_json(run / "manifest.json", {
                    "analysis_manifest_version": 1, "analysis_run_id": run_id,
                    "status": "failed", "failed_at": utc_now(),
                    "error_type": type(error).__name__})
            raise
        finally:
            lock.unlink(missing_ok=True)

    def source(self, run_id: str) -> tuple[dict[str, Any], Path, list[dict[str, Any]]]:
        """Validate source identity, reconciliation, original input hash and Silver paths."""
        validate_run_id(run_id)
        run = safe_path(self.root, f"runs/{run_id}")
        manifest = read_json(safe_path(run, "manifest.json"))
        if (type(manifest.get("manifest_version")) is not int
                or manifest.get("manifest_version") != 1 or manifest.get("run_id") != run_id
                or manifest.get("status") != "completed"
                or manifest.get("schema_version") != "payment_event_v1"
                or manifest.get("pipeline_version") != "1.0.0"
                or manifest.get("label_is_synthetic") is not True):
            raise ValueError("unsupported or incomplete source manifest")
        counts = manifest["counts"]
        if not all(type(counts.get(key)) is int and counts[key] >= 0 for key in
                   ("bronze", "silver_unique", "duplicates_removed", "quarantine", "gold")):
            raise ValueError("invalid source counts")
        if (manifest["input"]["rows"] != counts["bronze"]
                or counts["bronze"] != sum(counts[key] for key in
                    ("silver_unique", "duplicates_removed", "quarantine"))
                or manifest["reconciliation"].get("passed") is not True):
            raise ValueError("source reconciliation failed")
        start, end = (datetime.fromisoformat(manifest[key]) for key in ("started_at", "finished_at"))
        if start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0) or end < start:
            raise ValueError("source timestamps must be ordered UTC")
        input_path = Path(manifest["input"]["path"])
        if input_path.is_absolute():
            input_path = input_path.relative_to(self.root)
        source_input = safe_path(self.root, str(input_path))
        if digest(source_input) != manifest["input"]["sha256"]:
            raise ValueError("source input checksum mismatch")
        if not {"silver", "bronze", "quarantine", "gold"} <= manifest["outputs"].keys():
            raise ValueError("source layer outputs are incomplete")
        for path in manifest["outputs"].values():
            output = safe_path(self.root, path)
            if not output.is_relative_to(run) or not output.is_dir():
                raise ValueError("source output must belong to selected run")
        silver = safe_path(self.root, manifest["outputs"]["silver"])
        return manifest, silver, inventory(self.root, silver)

    def provenance(self, source: dict[str, Any]) -> dict[str, Any]:
        """Use generator metadata only when its digest/count bind it to the source."""
        unknown = {"seed": "unknown", "expected_count": "unknown"}
        candidate = Path(source["input"]["path"]).with_suffix(".manifest.json")
        if candidate.is_absolute():
            candidate = candidate.relative_to(self.root)
        path = safe_path(self.root, str(candidate), exists=False)
        if not path.exists():
            return unknown
        generated = read_json(path)
        if (generated.get("manifest_version") != 1 or generated.get("status") != "completed"
                or generated.get("label_is_synthetic") is not True
                or generated.get("sha256") != source["input"]["sha256"]
                or generated.get("count_written") != source["input"]["rows"]
                or type(generated.get("seed")) is not int):
            return unknown
        return {"seed": generated["seed"], "expected_count": generated["count_requested"],
                "generator_manifest": generated, "generator_manifest_sha256": digest(path)}

    def completed(self, run_id: str) -> tuple[Path, dict[str, Any]]:
        """Verify a published run and every artifact before reuse/comparison."""
        validate_run_id(run_id)
        run = safe_path(self.root, f"analysis-runs/{run_id}")
        value = read_json(safe_path(run, "manifest.json"))
        if (type(value.get("analysis_manifest_version")) is not int
                or value.get("analysis_manifest_version") != 1 or value.get("status") != "completed"
                or value.get("analysis_run_id") != run_id
                or value.get("protocol_id") != PROTOCOL_ID):
            raise ValueError("analysis manifest is not supported/completed")
        verify_records(run, value["artifacts"])
        required = {"protocol.json", "source-manifest.json", "source-files.json",
                    "metrics.json", "dashboard-summary.json"}
        if not required <= {record["path"] for record in value["artifacts"]}:
            raise ValueError("missing required artifact checksum")
        if digest(run / "source-manifest.json") != value["source_manifest_sha256"]:
            raise ValueError("source manifest checksum mismatch")
        config = read_json(run / "protocol.json")
        rule = RuleConfig.from_dict(config["rule"])
        if config != protocol(rule) or sha256(canonical_bytes(config)).hexdigest() != value["config_sha256"]:
            raise ValueError("protocol checksum mismatch")
        summary = read_json(run / "dashboard-summary.json")
        metrics = read_json(run / "metrics.json")
        counts = value["counts"]
        if (not all(type(count) is int and count >= 0 for count in counts.values())
                or not all(counts[key] == counts["N"] for key in ("source_silver", "predictions", "evaluation"))
                or counts["N"] != sum(counts[key] for key in ("TP", "FP", "FN", "TN"))
                or counts["P"] != counts["TP"] + counts["FN"]
                or counts["Nneg"] != counts["TN"] + counts["FP"]
                or counts["alerts"] != counts["TP"] + counts["FP"]):
            raise ValueError("analysis counts do not reconcile")
        if (summary.get("label_is_synthetic") is not True or len(summary["examples"]) > 20
                or summary["counts"] != value["counts"] or metrics["counts"] != value["counts"]
                or summary["metrics"] != metrics["metrics"]):
            raise ValueError("summary does not reconcile")
        for name in ("predictions", "evaluation"):
            actual = inventory(run, run / name)
            expected = [record for record in value["artifacts"] if record["path"].startswith(name + "/")]
            if actual != expected:
                raise ValueError("Parquet inventory mismatch")
        return run, value
