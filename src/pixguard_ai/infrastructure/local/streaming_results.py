"""Bounded validation of Kafka transport manifests for dashboard display."""

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
from typing import Any


MAX_MANIFEST_BYTES = 65_536
MAX_DIRECTORIES = 10_000
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


@dataclass(frozen=True)
class StreamingSnapshot:
    """Latest observed transport stages, each kept independent."""

    publication: dict[str, Any] | None
    capture: dict[str, Any] | None
    snapshot: dict[str, Any] | None
    message: str
    error: bool = False


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject ambiguous duplicate JSON keys."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _read_json(path: Path) -> dict[str, Any]:
    """Read one bounded, symlink-free JSON manifest."""
    if path.is_symlink():
        raise ValueError("manifest is a symlink")
    payload = path.read_bytes()
    if len(payload) > MAX_MANIFEST_BYTES:
        raise ValueError("manifest exceeds limit")
    value = json.loads(payload, object_pairs_hook=_unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    if not isinstance(value, dict):
        raise ValueError("manifest must be an object")
    return value


def _timestamp(value: object) -> datetime:
    """Require an explicit UTC timestamp."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must use UTC")
    return parsed


def _integer(value: object) -> bool:
    """Accept only plain nonnegative JSON integers."""
    return type(value) is int and value >= 0


def _publication(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        raise ValueError("publication manifest is invalid")
    required = {"manifest_version", "source_batch_id", "status", "confirmed",
                "uncertain", "not_sent", "total", "started_at"}
    if not required <= value.keys():
        raise ValueError("publication manifest is incomplete")
    if type(value["manifest_version"]) is not int or value["manifest_version"] != 1:
        raise ValueError("publication manifest version is unsupported")
    if not isinstance(value["source_batch_id"], str) or not ID.fullmatch(value["source_batch_id"]):
        raise ValueError("publication batch ID is invalid")
    if not all(_integer(value.get(name)) for name in ("confirmed", "uncertain", "not_sent", "total")):
        raise ValueError("publication counts are invalid")
    if value["confirmed"] + value["uncertain"] + value["not_sent"] != value["total"]:
        raise ValueError("publication counts do not reconcile")
    if value["status"] not in {"validating", "publishing", "completed", "interrupted", "failed"}:
        raise ValueError("publication status is invalid")
    if value["status"] == "completed" and (value["confirmed"] != value["total"]
                                            or value["uncertain"] != 0 or value["not_sent"] != 0):
        raise ValueError("completed publication is incomplete")
    _timestamp(value["started_at"])
    if value.get("finished_at") is not None:
        _timestamp(value["finished_at"])
    if value.get("source_sha256") is not None and (
            not isinstance(value["source_sha256"], str)
            or SHA256.fullmatch(value["source_sha256"]) is None):
        raise ValueError("source checksum is invalid")
    if value.get("error") is not None and not isinstance(value["error"], str):
        raise ValueError("publication error is invalid")
    return value


def _capture(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        raise ValueError("capture manifest is invalid")
    required = {"capture_manifest_version", "capture_id", "status", "cluster_id",
                "topic", "partition", "start", "count", "end", "started_at"}
    if not required <= value.keys():
        raise ValueError("capture manifest is incomplete")
    if type(value["capture_manifest_version"]) is not int or value["capture_manifest_version"] != 1:
        raise ValueError("capture manifest version is unsupported")
    if not isinstance(value["capture_id"], str) or not ID.fullmatch(value["capture_id"]):
        raise ValueError("capture ID is invalid")
    if value["status"] not in {"capturing", "completed", "failed", "interrupted"}:
        raise ValueError("capture status is invalid")
    for name in ("partition", "start", "count"):
        if not _integer(value[name]):
            raise ValueError("capture offset/count is invalid")
    if value["end"] is not None and (not _integer(value["end"]) or value["end"] < value["start"]):
        raise ValueError("capture range is invalid")
    if not isinstance(value["cluster_id"], str) or not value["cluster_id"]:
        raise ValueError("cluster identity is invalid")
    if not isinstance(value["topic"], str) or not value["topic"]:
        raise ValueError("topic identity is invalid")
    _timestamp(value["started_at"])
    if value.get("finished_at") is not None:
        _timestamp(value["finished_at"])
    if value["status"] == "completed":
        if value["end"] is None or not isinstance(value.get("segment"), dict):
            raise ValueError("completed capture is incomplete")
        segment = value["segment"]
        if (not isinstance(segment.get("sha256"), str)
                or SHA256.fullmatch(segment["sha256"]) is None
                or not _integer(segment.get("size_bytes"))):
            raise ValueError("capture segment checksum is invalid")
    if value.get("error") is not None and not isinstance(value["error"], str):
        raise ValueError("capture error is invalid")
    return value


def _snapshot(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        raise ValueError("snapshot manifest is invalid")
    required = {"snapshot_manifest_version", "snapshot_id", "capture_id", "status",
                "capture", "counts", "reconciliation", "finished_at",
                "events_sha256", "quarantine_sha256"}
    if not required <= value.keys():
        raise ValueError("snapshot manifest is incomplete")
    if type(value["snapshot_manifest_version"]) is not int or value["snapshot_manifest_version"] != 1:
        raise ValueError("snapshot manifest version is unsupported")
    if not isinstance(value["snapshot_id"], str) or not ID.fullmatch(value["snapshot_id"]):
        raise ValueError("snapshot ID is invalid")
    if not isinstance(value["capture_id"], str) or not ID.fullmatch(value["capture_id"]):
        raise ValueError("snapshot capture ID is invalid")
    if value["status"] != "completed":
        raise ValueError("snapshot status is invalid")
    counts = value["counts"]
    names = ("physical", "exported", "duplicates",
             "transport_quarantine", "contract_quarantine")
    if not isinstance(counts, dict) or not all(_integer(counts.get(name)) for name in names):
        raise ValueError("snapshot counts are invalid")
    if counts["physical"] != sum(counts[name] for name in names[1:]):
        raise ValueError("snapshot counts do not reconcile")
    reconciliation = value["reconciliation"]
    if not isinstance(reconciliation, dict) or reconciliation.get("passed") is not True:
        raise ValueError("snapshot reconciliation failed")
    if not isinstance(value["capture"], dict):
        raise ValueError("snapshot capture provenance is invalid")
    for name in ("events_sha256", "quarantine_sha256"):
        if not isinstance(value[name], str) or SHA256.fullmatch(value[name]) is None:
            raise ValueError("snapshot checksum is invalid")
    _timestamp(value["finished_at"])
    return value


def _latest(directory: Path, manifest_name: str,
           validator: Any) -> dict[str, Any] | None:
    """Find the latest valid manifest by bounded directory traversal."""
    if not directory.exists() or directory.is_symlink():
        return None
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    invalid_manifest = False
    directories = sorted(directory.iterdir(), key=lambda path: path.name)
    if len(directories) > MAX_DIRECTORIES:
        raise ValueError("manifest namespace exceeds directory limit")
    for item in directories[:MAX_DIRECTORIES]:
        if item.is_symlink() or not item.is_dir():
            continue
        manifest_path = item / manifest_name
        if not manifest_path.is_file() or manifest_path.is_symlink():
            continue
        try:
            value = validator(_read_json(manifest_path))
            stamp = value.get("finished_at") or value.get("started_at")
            _timestamp(stamp)
            candidates.append((_timestamp(stamp), value))
        except (ValueError, json.JSONDecodeError, OSError, OverflowError):
            invalid_manifest = True
            continue
    if not candidates:
        if invalid_manifest:
            raise ValueError("manifest namespace contains invalid metadata")
        return None
    candidates.sort(key=lambda pair: pair[0], reverse=True)
    return candidates[0][1]


def load_latest_streaming_results(root: Path) -> StreamingSnapshot:
    """Load only latest valid publication, capture and snapshot manifests."""
    try:
        if root.is_symlink():
            raise ValueError("data root is a symlink")
        root = root.resolve(strict=True)
        publication = _latest(root / "publications", "manifest.json", _publication)
        capture = _latest(root / "captures", "manifest.json", _capture)

        snapshot_candidates: list[tuple[datetime, dict[str, Any]]] = []
        invalid_snapshot = False
        captures = root / "captures"
        if captures.exists() and not captures.is_symlink():
            capture_dirs = sorted(captures.iterdir(), key=lambda path: path.name)
            if len(capture_dirs) > MAX_DIRECTORIES:
                raise ValueError("capture namespace exceeds limit")
            for capture_dir in capture_dirs[:MAX_DIRECTORIES]:
                snapshots = capture_dir / "snapshots"
                if snapshots.is_symlink() or not snapshots.is_dir():
                    continue
                for snapshot_dir in snapshots.iterdir():
                    manifest_path = snapshot_dir / "manifest.json"
                    if snapshot_dir.is_symlink() or manifest_path.is_symlink():
                        continue
                    if not manifest_path.is_file():
                        continue
                    try:
                        value = _snapshot(_read_json(manifest_path))
                        snapshot_candidates.append((_timestamp(value["finished_at"]), value))
                    except (ValueError, json.JSONDecodeError, OSError, OverflowError):
                        invalid_snapshot = True
                        continue
        snapshot = None
        if snapshot_candidates:
            snapshot_candidates.sort(key=lambda pair: pair[0], reverse=True)
            snapshot = snapshot_candidates[0][1]
        elif invalid_snapshot:
            raise ValueError("snapshot namespace contains invalid metadata")
        if publication is None and capture is None and snapshot is None:
            return StreamingSnapshot(None, None, None,
                                     "Nenhum transporte Kafka publicado ou capturado disponível.")
        return StreamingSnapshot(publication, capture, snapshot,
                                 "Manifestos de transporte Kafka disponíveis.")
    except FileNotFoundError:
        return StreamingSnapshot(None, None, None,
                                 "Nenhum transporte Kafka disponível: raiz de dados inexistente.")
    except (ValueError, json.JSONDecodeError, OSError, OverflowError):
        return StreamingSnapshot(None, None, None,
                                 "Manifestos de transporte inválidos ou incompatíveis.", True)
