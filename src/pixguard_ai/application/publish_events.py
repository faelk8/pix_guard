"""Bounded publication with conservative, durable accounting across crashes."""

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from time import monotonic
from typing import Callable, Iterable, Protocol


@dataclass(frozen=True)
class SourceInfo:
    """Validated immutable source identity."""

    count: int
    sha256: str


class EventSource(Protocol):
    """Snapshot and validate before exposing wire records."""

    def prepare(self) -> SourceInfo: ...
    def records(self) -> Iterable[tuple[str, bytes]]: ...
    def verify(self) -> None: ...


class Publisher(Protocol):
    """Broker boundary; callbacks report only acknowledged coordinates."""

    def inspect(self, timeout: float) -> dict[str, object]: ...
    def send(self, key: str, payload: bytes, headers: dict[str, str],
             callback: Callable[[dict[str, object] | None], None]) -> None: ...
    def flush(self, timeout: float) -> int: ...


class PublicationStore(Protocol):
    """Exclusive run reservation and durable atomic checkpoints."""

    def reserve(self, batch_id: str) -> None: ...
    def checkpoint(self, manifest: dict[str, object]) -> None: ...
    def receipts(self, window: int, records: list[dict[str, object]]) -> None: ...


def publish_events(batch_id: str, source: EventSource, publisher: Publisher,
                   store: PublicationStore, timeout: float = 60.0,
                   window_size: int = 100) -> dict[str, object]:
    """Never report success unless every ordinal has a durable broker receipt.

    Before a window is offered to Kafka it is durably marked uncertain. A crash
    therefore leaves an incomplete manifest, including conservative uncertainty.
    Reusing a batch ID is prohibited; another ID represents republication.
    """
    if not 0 < timeout <= 3600 or not 1 <= window_size <= 1000:
        raise ValueError("Invalid timeout or window size")
    started_at = datetime.now(timezone.utc).isoformat()
    store.reserve(batch_id)
    manifest: dict[str, object] = {
        "manifest_version": 1, "transport_version": "1", "source_batch_id": batch_id,
        "status": "validating", "confirmed": 0, "uncertain": 0, "not_sent": 0,
        "total": 0, "error": None, "started_at": started_at, "finished_at": None,
    }
    store.checkpoint(manifest)
    confirmed = 0
    uncertain = 0
    total = 0
    try:
        info = source.prepare()
        total = info.count
        manifest.update(total=total, not_sent=total, source_sha256=info.sha256)
        started = monotonic()
        metadata = publisher.inspect(timeout)
        manifest.update(metadata, status="publishing")
        store.checkpoint(manifest)
        iterator = iter(source.records())
        ordinal = 0
        window = 0
        while ordinal < total:
            records = []
            for _ in range(min(window_size, total - ordinal)):
                key, payload = next(iterator)
                records.append((ordinal, key, payload))
                ordinal += 1
            if monotonic() - started >= timeout:
                raise TimeoutError("Publication deadline exceeded")
            # Persist potential delivery before entering any external side effect.
            uncertain = len(records)
            manifest.update(uncertain=uncertain, not_sent=total-confirmed-uncertain)
            store.checkpoint(manifest)
            receipts: list[dict[str, object]] = []
            attempted = 0
            try:
                for index, key, payload in records:
                    attempted += 1
                    def delivered(coordinate: dict[str, object] | None,
                                  index: int = index, key: str = key,
                                  payload_hash: str = sha256(payload).hexdigest()) -> None:
                        if coordinate is not None:
                            receipts.append(dict(coordinate, source_ordinal=index, event_id=key,
                                                 payload_sha256=payload_hash))
                    publisher.send(key, payload, {
                        "source_batch_id": batch_id, "source_sha256": info.sha256,
                        "source_ordinal": str(index), "transport_version": "1",
                    }, delivered)
                pending = publisher.flush(max(0.0, timeout - (monotonic() - started)))
                if pending or len(receipts) != len(records):
                    raise TimeoutError("Delivery is uncertain")
            finally:
                # Receipt publication precedes counts. Persistence failure leaves
                # the older conservative manifest authoritative, never success.
                store.receipts(window, receipts)
                confirmed += len(receipts)
                uncertain = attempted - len(receipts)
                manifest.update(confirmed=confirmed, uncertain=uncertain,
                                not_sent=total-confirmed-uncertain)
                store.checkpoint(manifest)
            window += 1
        source.verify()
        manifest.update(status="completed", confirmed=confirmed, uncertain=0, not_sent=0,
                        finished_at=datetime.now(timezone.utc).isoformat())
        store.checkpoint(manifest)
    except BaseException as error:
        manifest.update(status="interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                        error=type(error).__name__, finished_at=datetime.now(timezone.utc).isoformat())
        store.checkpoint(manifest)
        raise
    return manifest
