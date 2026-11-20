"""Application orchestration for exporting a captured Kafka range."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from pixguard_ai.domain.capture import validate_capture_id, validate_snapshot_id


@dataclass(frozen=True)
class ExportRequest:
    """Immutable request for one export of a completed capture."""

    capture_id: str
    snapshot_id: str
    data_root: Path


class CaptureReader(Protocol):
    """Read and verify the completed capture source."""

    def read_completed(self, capture_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Return verified manifest and raw ordered records."""
        ...


class SnapshotStore(Protocol):
    """Publish an immutable snapshot and its provenance."""

    def reserve(self, capture_id: str, snapshot_id: str) -> None:
        """Reserve an exclusive snapshot namespace."""
        ...

    def checkpoint(self, manifest: dict[str, Any]) -> None:
        """Persist a bounded export checkpoint."""
        ...

    def publish(self, capture_manifest: dict[str, Any],
                exported: list[bytes], transport_quarantine: list[dict[str, Any]],
                contract_quarantine: list[dict[str, Any]],
                duplicate_count: int) -> dict[str, Any]:
        """Publish final snapshot files and a verified provenance manifest."""
        ...


def export_capture(request: ExportRequest, reader: CaptureReader,
                   store: SnapshotStore) -> dict[str, Any]:
    """Validate, classify, reconcile and publish one immutable snapshot."""
    capture_id = validate_capture_id(request.capture_id)
    snapshot_id = validate_snapshot_id(request.snapshot_id)
    capture_manifest, records = reader.read_completed(capture_id)
    store.reserve(capture_id, snapshot_id)

    transport_quarantine: list[dict[str, Any]] = []
    contract_quarantine: list[dict[str, Any]] = []
    valid_by_id: dict[str, list[tuple[bytes, dict[str, Any]]]] = {}

    import base64
    import json
    from pixguard_ai.domain.events import PaymentEvent

    def quarantine(transport: bool, record: dict[str, Any], reason: str) -> None:
        item = {"capture_offset": record["offset"], "reason": reason,
                "record_checksum_sha256": record["checksum_sha256"]}
        (transport_quarantine if transport else contract_quarantine).append(item)

    for record in records:
        offset = record["offset"]
        key = record.get("key_base64")
        value = record.get("value_base64")
        headers = record.get("headers")
        if key is None:
            quarantine(True, record, "missing_key")
            continue
        if value is None:
            quarantine(True, record, "tombstone")
            continue
        if not isinstance(headers, list):
            quarantine(True, record, "invalid_headers")
            continue
        try:
            key_bytes = base64.b64decode(key, validate=True)
            value_bytes = base64.b64decode(value, validate=True)
        except Exception:
            quarantine(True, record, "invalid_base64")
            continue
        try:
            text = value_bytes.decode("utf-8")
        except UnicodeDecodeError:
            quarantine(True, record, "not_utf8")
            continue
        if "\n" in text or "\r" in text:
            quarantine(True, record, "multiline_payload")
            continue
        try:
            parsed = json.loads(text)
            event = PaymentEvent.from_dict(parsed)
        except Exception:
            quarantine(False, record, "invalid_payment_event_contract")
            continue

        header_map: dict[str, bytes | None] = {}
        seen_headers: set[str] = set()
        for header in headers:
            if not isinstance(header, list) or len(header) != 2 or not isinstance(header[0], str):
                quarantine(True, record, "invalid_headers")
                break
            if header[0] in seen_headers:
                quarantine(True, record, "duplicate_transport_header")
                break
            seen_headers.add(header[0])
            payload = header[1]
            try:
                decoded = None if payload is None else base64.b64decode(payload, validate=True)
            except Exception:
                quarantine(True, record, "invalid_headers")
                break
            header_map[header[0]] = decoded
        else:
            if header_map.get("transport_version") != b"1":
                quarantine(True, record, "invalid_transport_version")
                continue
            try:
                required = ("source_batch_id", "source_sha256", "source_ordinal")
                if any(name not in header_map for name in required):
                    quarantine(True, record, "missing_transport_headers")
                    continue
            except Exception:
                quarantine(True, record, "invalid_headers")
                continue
            try:
                key_text = key_bytes.decode("utf-8")
            except UnicodeDecodeError:
                quarantine(True, record, "invalid_key_utf8")
                continue
            if key_text != str(event.event_id):
                quarantine(True, record, "key_event_id_mismatch")
                continue
            canonical = json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":"),
                                   ensure_ascii=False).encode("utf-8")
            valid_by_id.setdefault(str(event.event_id), []).append((canonical, record))
            continue

    exported: list[bytes] = []
    duplicate_count = 0
    for event_id, occurrences in sorted(valid_by_id.items()):
        payloads = {payload for payload, _ in occurrences}
        if len(payloads) > 1:
            for payload, record in occurrences:
                quarantine(False, record, "conflicting_event_id_payload")
            continue
        exported.append(next(iter(payloads)) + b"\n")
        duplicate_count += len(occurrences) - 1
        if len(occurrences) > 1:
            for _payload, record in occurrences[1:]:
                record["duplicate_of_offset"] = occurrences[0][1]["offset"]

    return store.publish(capture_manifest, exported, transport_quarantine,
                         contract_quarantine, duplicate_count)
