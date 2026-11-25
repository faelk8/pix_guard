"""Application orchestration for exporting a captured Kafka range."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager, ExitStack
import itertools
import json

from pixguard_ai.domain.capture import validate_capture_id, validate_snapshot_id


class ExportRecords(Protocol):
    """Replayable export working set owned by the storage adapter."""

    def append(self, value: Any) -> None:
        """Append one intermediate record."""
        ...

    def clear(self) -> None:
        """Reset a reusable intermediate group."""
        ...

    def __iter__(self) -> Iterator[Any]:
        """Read intermediate records in order."""
        ...


class ExportWorkspace(Protocol):
    """Provide bounded working storage independently of its implementation."""

    def collection(self) -> ExportRecords:
        """Allocate a collection released when the workspace exits."""
        ...

    def ordered(self, records: Iterable[Any], key: Callable[[Any], Any]) -> Iterator[Any]:
        """Sort records without retaining the entire working set."""
        ...


@dataclass(frozen=True)
class ExportRequest:
    """Immutable request for one export of a completed capture."""

    capture_id: str
    snapshot_id: str
    data_root: Path


class CaptureReader(Protocol):
    """Read and verify the completed capture source."""

    def read_completed(self, capture_id: str) -> tuple[dict[str, Any], Iterable[dict[str, Any]]]:
        """Return verified manifest and raw ordered records."""
        ...


class SnapshotStore(Protocol):
    """Publish an immutable snapshot and its provenance."""

    def workspace(self) -> AbstractContextManager[ExportWorkspace]:
        """Manage temporary resources for one export."""
        ...

    def reserve(self, capture_id: str, snapshot_id: str) -> None:
        """Reserve an exclusive snapshot namespace."""
        ...

    def checkpoint(self, manifest: dict[str, Any]) -> None:
        """Persist a bounded export checkpoint."""
        ...

    def publish(self, capture_manifest: dict[str, Any],
                exported: Iterable[bytes], transport_quarantine: Iterable[dict[str, Any]],
                contract_quarantine: Iterable[dict[str, Any]],
                duplicate_count: int) -> dict[str, Any]:
        """Publish final snapshot files and a verified provenance manifest."""
        ...


def export_capture(request: ExportRequest, reader: CaptureReader,
                   store: SnapshotStore) -> dict[str, Any]:
    """Validate, classify, reconcile and publish one immutable snapshot."""
    with ExitStack() as scratch:
        workspace = scratch.enter_context(store.workspace())
        return _export_capture(request, reader, store, scratch, workspace)


def _export_capture(request: ExportRequest, reader: CaptureReader,
                    store: SnapshotStore, scratch: ExitStack,
                    workspace: ExportWorkspace) -> dict[str, Any]:
    """Keep all temporary collections alive until publication finishes."""
    capture_id = validate_capture_id(request.capture_id)
    snapshot_id = validate_snapshot_id(request.snapshot_id)
    capture_manifest, records = reader.read_completed(capture_id)
    close_records = getattr(records, "close", None)
    if close_records is not None:
        scratch.callback(close_records)
    store.reserve(capture_id, snapshot_id)

    transport_quarantine = workspace.collection()
    contract_quarantine = workspace.collection()
    valid_records = workspace.collection()

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
            valid_records.append([str(event.event_id), canonical.decode("utf-8"),
                                  record["offset"], record["checksum_sha256"]])
            continue

    exported = workspace.collection()
    duplicate_count = 0
    # A single event ID can occur arbitrarily often: spool its coordinates too.
    occurrences = workspace.collection()
    ordered = workspace.ordered(valid_records, key=lambda item: (item[0], item[2]))
    for _event_id, group in itertools.groupby(ordered, key=lambda item: item[0]):
        occurrences.clear()
        first_payload = None
        conflict = False
        count = 0
        for _identifier, payload, offset, checksum in group:
            if first_payload is None:
                first_payload = payload
            conflict = conflict or payload != first_payload
            count += 1
            occurrences.append({"offset": offset, "checksum_sha256": checksum})
        if conflict:
            for record in occurrences:
                quarantine(False, record, "conflicting_event_id_payload")
        else:
            exported.append(first_payload)
            duplicate_count += count - 1

    return store.publish(capture_manifest,
                         (payload.encode("utf-8") + b"\n" for payload in exported),
                         transport_quarantine, contract_quarantine, duplicate_count)
