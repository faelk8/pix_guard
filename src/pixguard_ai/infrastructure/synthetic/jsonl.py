"""Incremental local JSONL persistence with atomic progress snapshots.

The manifest lives beside the requested output (events.jsonl -> events.manifest.json).
It reports flushed records, real execution timestamps and a checksum of those bytes.
Only a completed stream replaces the published JSONL. Failed partial output remains
for inspection and a new run starts from zero; replay replaces instead of appending.
An advisory per-output lock rejects simultaneous writers to the same destination.
"""

from datetime import datetime, timezone
import fcntl
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from time import monotonic
from typing import Iterable, Mapping

from pixguard_ai.application.generate_events import GenerationConfig, GenerationManifest
from pixguard_ai.domain.events import PaymentEvent, SCENARIOS, SCHEMA_VERSION


def manifest_path(output: Path) -> Path:
    """Derive the dashboard's stable sidecar path from its JSONL destination."""
    return output.with_suffix(".manifest.json")


def _now() -> str:
    """Return an actual observation time, independent of simulated event times."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, value: Mapping[str, object]) -> None:
    """Prevent readers from observing a partially written manifest."""
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class JsonlEventWriter:
    """Persist one event per line; memory use does not grow with dataset size."""

    def __init__(self, output: Path, progress_interval: int = 1_000) -> None:
        """Configure the destination and the maximum records between snapshots."""
        if output.suffix != ".jsonl":
            raise ValueError("output must have the .jsonl suffix")
        if type(progress_interval) is not int or progress_interval < 1:
            raise ValueError("progress_interval must be a positive integer")
        self.output = output
        self.progress_interval = progress_interval

    def write(self, events: Iterable[PaymentEvent], config: GenerationConfig) -> GenerationManifest:
        """Serialize a single writer's stream, preserving previous output on failure."""
        self.output.parent.mkdir(parents=True, exist_ok=True)
        with self.output.with_suffix(".lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ValueError("Generation is already running for this output") from error
            return self._write_locked(events, config)

    def _write_locked(
        self, events: Iterable[PaymentEvent], config: GenerationConfig,
    ) -> GenerationManifest:
        """Count only written records and publish completed data atomically."""
        partial = self.output.with_suffix(".jsonl.partial")
        path = manifest_path(self.output)
        started = monotonic()
        checksum = sha256()
        scenario_counts = dict.fromkeys(SCENARIOS, 0)
        written = 0
        manifest: GenerationManifest = {
            "manifest_version": 1,
            "schema_version": SCHEMA_VERSION,
            "label_is_synthetic": True,
            "status": "running",
            "seed": config.seed,
            "count_requested": config.count,
            "count_written": 0,
            "progress": 0.0,
            "output_path": str(self.output),
            "partial_path": str(partial),
            "started_at": _now(),
            "updated_at": _now(),
            "finished_at": None,
            "elapsed_seconds": 0.0,
            "scenario_counts": scenario_counts,
            "bytes_written": 0,
            "sha256": checksum.hexdigest(),
            "error": None,
        }

        def snapshot(status: str) -> None:
            """Report observed values after flushing the corresponding data."""
            manifest.update(
                status=status, count_written=written, progress=written / config.count,
                updated_at=_now(), elapsed_seconds=round(monotonic() - started, 6),
                sha256=checksum.hexdigest(),
            )
            if status != "running":
                manifest["finished_at"] = _now()
            _atomic_json(path, manifest)

        try:
            with partial.open("wb") as stream:
                snapshot("running")
                try:
                    for event in events:
                        if written >= config.count:
                            raise ValueError("Source produced more records than requested")
                        if not isinstance(event, PaymentEvent):
                            raise ValueError("Source must yield validated PaymentEvent objects")
                        encoded = json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":"))
                        line = (encoded + "\n").encode("utf-8")
                        stream.write(line)
                        checksum.update(line)
                        written += 1
                        scenario_counts[event.ground_truth.scenario_id] += 1
                        manifest["bytes_written"] += len(line)
                        if written % self.progress_interval == 0:
                            stream.flush()
                            snapshot("running")
                    if written != config.count:
                        raise ValueError("Source produced fewer records than requested")
                finally:
                    stream.flush()
                    os.fsync(stream.fileno())
            os.replace(partial, self.output)
            snapshot("completed")
        except BaseException as error:
            # Deliberately avoid recording arbitrary exception strings or secrets.
            manifest["error"] = type(error).__name__
            snapshot("interrupted" if isinstance(error, KeyboardInterrupt) else "failed")
            raise
        return manifest
