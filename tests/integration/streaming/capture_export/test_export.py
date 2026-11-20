"""Docker integration coverage for repeated capture snapshot export."""

import base64
from hashlib import sha256
import json
from pathlib import Path

from pixguard_ai.application.export_capture import ExportRequest, export_capture
from pixguard_ai.domain.events import PaymentEvent
from pixguard_ai.infrastructure.local.capture_snapshot import CaptureSnapshot, canonical


def test_repeated_export_is_immutable_and_reconciled(tmp_path: Path) -> None:
    capture_dir = tmp_path / "captures" / "capture-replay"
    capture_dir.mkdir(parents=True)
    event = {
        "event_id": "12345678-1234-5234-9234-567812345678",
        "event_time": "2026-11-20T10:00:00+00:00",
        "ingested_at": "2026-11-20T10:00:01+00:00",
        "features": {
            "account_id_hash": "0" * 64,
            "device_id_hash": "1" * 64,
            "merchant_id": "merchant_0001",
            "amount_brl": "10.25",
            "location_region": "region_01",
            "channel": "mobile",
            "status": "completed",
        },
        "ground_truth": {"scenario_id": "normal", "is_suspicious": False},
        "schema_version": "payment_event_v1",
        "label_is_synthetic": True,
    }
    value = json.dumps(event, separators=(",", ":")).encode()
    records = []
    for offset in (0, 1):
        record = {
            "topic": "pixguard.payment_event_v1",
            "partition": 0,
            "offset": offset,
            "key_base64": base64.b64encode(event["event_id"].encode()).decode(),
            "value_base64": base64.b64encode(value).decode(),
            "headers": [
                ["source_batch_id", base64.b64encode(b"batch").decode()],
                ["source_sha256", base64.b64encode(b"a" * 64).decode()],
                ["source_ordinal", base64.b64encode(str(offset).encode()).decode()],
                ["transport_version", base64.b64encode(b"1").decode()],
            ],
            "timestamp": 1700000000000,
        }
        record["checksum_sha256"] = sha256(canonical(record)).hexdigest()
        records.append(record)
    segment = capture_dir / "records.jsonl"
    segment.write_bytes(b"".join(canonical(record) + b"\n" for record in records))
    (capture_dir / "manifest.json").write_text(json.dumps({
        "capture_manifest_version": 1,
        "capture_id": "capture-replay",
        "cluster_id": "cluster-1",
        "topic": "pixguard.payment_event_v1",
        "partition": 0,
        "start": 0,
        "end": 2,
        "status": "completed",
        "count": 2,
        "segment": {
            "path": "records.jsonl",
            "sha256": sha256(segment.read_bytes()).hexdigest(),
            "size_bytes": segment.stat().st_size,
        },
    }), encoding="utf-8")

    storage = CaptureSnapshot(tmp_path)
    first = export_capture(
        ExportRequest("capture-replay", "snapshot-one", tmp_path), storage, storage)
    second = export_capture(
        ExportRequest("capture-replay", "snapshot-two", tmp_path),
        CaptureSnapshot(tmp_path),
        CaptureSnapshot(tmp_path),
    )
    one = tmp_path / "captures" / "capture-replay" / "snapshots" / "snapshot-one"
    two = tmp_path / "captures" / "capture-replay" / "snapshots" / "snapshot-two"
    assert one.joinpath("events.jsonl").read_bytes() == two.joinpath("events.jsonl").read_bytes()
    assert one.joinpath("quarantine.jsonl").read_bytes() == two.joinpath("quarantine.jsonl").read_bytes()
    assert first["counts"] == second["counts"]
    assert first["counts"] == {
        "physical": 2,
        "exported": 1,
        "duplicates": 1,
        "transport_quarantine": 0,
        "contract_quarantine": 0,
    }
    exported_event = json.loads(one.joinpath("events.jsonl").read_text().splitlines()[0])
    assert PaymentEvent.from_dict(exported_event).event_id == PaymentEvent.from_dict(event).event_id
