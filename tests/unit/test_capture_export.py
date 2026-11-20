"""Unit coverage for capture export, quarantine and snapshot reconciliation."""

import base64
from hashlib import sha256
import json
from pathlib import Path

import pytest

from pixguard_ai.application.export_capture import ExportRequest, export_capture
from pixguard_ai.infrastructure.local.capture_snapshot import CaptureSnapshot, canonical


def event(event_id: str, merchant: str = "merchant_0001") -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_time": "2026-11-20T10:00:00+00:00",
        "ingested_at": "2026-11-20T10:00:01+00:00",
        "features": {
            "account_id_hash": "0" * 64,
            "device_id_hash": "1" * 64,
            "merchant_id": merchant,
            "amount_brl": "10.25",
            "location_region": "region_01",
            "channel": "mobile",
            "status": "completed",
        },
        "ground_truth": {"scenario_id": "normal", "is_suspicious": False},
        "schema_version": "payment_event_v1",
        "label_is_synthetic": True,
    }


def capture_record(offset: int, value: bytes | None, key: bytes | None,
                   headers: list[list[str | None]] | None = None) -> dict[str, object]:
    record = {
        "topic": "pixguard.payment_event_v1",
        "partition": 0,
        "offset": offset,
        "key_base64": None if key is None else base64.b64encode(key).decode(),
        "value_base64": None if value is None else base64.b64encode(value).decode(),
        "headers": headers if headers is not None else [
            ["source_batch_id", base64.b64encode(b"batch-1").decode()],
            ["source_sha256", base64.b64encode(b"a" * 64).decode()],
            ["source_ordinal", base64.b64encode(b"0").decode()],
            ["transport_version", base64.b64encode(b"1").decode()],
        ],
        "timestamp": 1700000000000,
    }
    record["checksum_sha256"] = sha256(canonical(record)).hexdigest()
    return record


def create_capture(tmp_path: Path, records: list[dict[str, object]]) -> None:
    capture_dir = tmp_path / "captures" / "capture-1"
    capture_dir.mkdir(parents=True)
    segment = capture_dir / "records.jsonl"
    lines = [canonical(record) + b"\n" for record in records]
    segment.write_bytes(b"".join(lines))
    manifest = {
        "capture_manifest_version": 1,
        "capture_id": "capture-1",
        "cluster_id": "cluster-1",
        "topic": "pixguard.payment_event_v1",
        "partition": 0,
        "start": 0,
        "end": len(records),
        "status": "completed",
        "count": len(records),
        "segment": {
            "path": "records.jsonl",
            "sha256": sha256(segment.read_bytes()).hexdigest(),
            "size_bytes": segment.stat().st_size,
        },
    }
    (capture_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_export_reconciles_valid_duplicate_conflict_and_quarantine(tmp_path: Path) -> None:
    event_id = "12345678-1234-5234-9234-567812345678"
    valid = json.dumps(event(event_id), separators=(",", ":")).encode()
    conflict = json.dumps(event(event_id, "merchant_0002"), separators=(",", ":")).encode()
    records = [
        capture_record(0, valid, event_id.encode()),
        capture_record(1, valid, event_id.encode()),
        capture_record(2, conflict, event_id.encode()),
        capture_record(3, b"\xff", b"key"),
        capture_record(4, None, b"key"),
        capture_record(5, b"{}", b"key"),
        capture_record(6, valid, b"\xff"),
        capture_record(7, valid, event_id.encode(), [
            ["source_batch_id", base64.b64encode(b"batch-1").decode()],
            ["source_sha256", base64.b64encode(b"a" * 64).decode()],
            ["source_ordinal", base64.b64encode(b"0").decode()],
            ["transport_version", base64.b64encode(b"1").decode()],
            ["transport_version", base64.b64encode(b"2").decode()],
        ]),
    ]
    create_capture(tmp_path, records)

    storage = CaptureSnapshot(tmp_path)
    manifest = export_capture(
        ExportRequest("capture-1", "snapshot-1", tmp_path),
        storage,
        storage,
    )

    assert manifest["status"] == "completed"
    snapshot = tmp_path / "captures" / "capture-1" / "snapshots" / "snapshot-1"
    assert snapshot.joinpath("events.jsonl").exists()
    quarantine = snapshot.joinpath("quarantine.jsonl").read_bytes().splitlines()
    assert manifest["counts"] == {
        "physical": 8,
        "exported": 0,
        "duplicates": 0,
        "transport_quarantine": 4,
        "contract_quarantine": 4,
    }
    assert len(quarantine) == 8
    assert manifest["reconciliation"]["passed"] is True
