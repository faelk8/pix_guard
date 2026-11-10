"""Spark SDP adapter with isolated runs and atomic manifests."""

from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import time
from typing import Any, BinaryIO, Iterator
import yaml

from pyspark.sql import SparkSession


class SparkRun:
    """Owns one SDP invocation and its immutable output directory."""

    def __init__(self, input_path: Path, data_root: Path, run_id: str) -> None:
        self.input_path = input_path.resolve(strict=True)
        self.data_root = data_root.resolve()
        self.run_id = run_id
        if not self.input_path.is_relative_to(self.data_root):
            raise ValueError("input must be inside /data/pixguard")
        self.run_dir = self.data_root / "runs" / run_id
        self.lock_path = self.data_root / "locks" / f"{run_id}.lock"
        if not self.run_dir.resolve().is_relative_to(self.data_root):
            raise ValueError("run path escapes data directory")

    def execute(self) -> dict[str, Any]:
        """Run actual SDP dry-run/materialization, reconcile datasets and publish manifest."""
        self.run_dir.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as error:
            raise RuntimeError("run is locked by another process") from error
        os.close(fd)
        started = time.monotonic()
        started_at = datetime.now(timezone.utc).isoformat()
        spark = None
        try:
            if self.run_dir.exists():
                raise FileExistsError("run_id already exists; choose a new identifier")
            self.run_dir.mkdir(mode=0o750)
            snapshot, input_hash, input_bytes = self._snapshot_input()
            self._verify_input_snapshot(snapshot, input_hash)
            project = self.run_dir / "sdp"
            project.mkdir()
            source_file = Path(__file__).with_name("transformations.py")
            shutil.copyfile(source_file, project / "transformations.py")
            spec = project / "spark-pipeline.yml"
            spec.write_text(yaml.safe_dump({
                "name": "pixguard_offline", "storage": (self.run_dir / "checkpoints").as_uri(),
                "libraries": [{"glob": {"include": "transformations.py"}}],
                "configuration": {"spark.pixguard.input": str(snapshot),
                                  "spark.pixguard.source_file": str(self.input_path),
                                  "spark.pixguard.run_id": self.run_id,
                                  "spark.sql.shuffle.partitions": "4",
                                  "spark.sql.session.timeZone": "UTC"},
            }, sort_keys=False), encoding="utf-8")
            self._invoke("dry-run", spec)
            self._invoke("run", spec)
            # SDP's local session catalog is transient; consume its Parquet paths directly.
            warehouse = self.run_dir / "spark-warehouse"
            tables = self._table_paths(warehouse)
            spark = SparkSession.builder.master("local[2]").appName(
                f"pixguard-reconcile-{self.run_id}").config(
                "spark.sql.warehouse.dir", str(warehouse)).config(
                "spark.sql.shuffle.partitions", "4").config(
                "spark.sql.session.timeZone", "UTC").getOrCreate()
            frames = {name: spark.read.parquet(str(path)) for name, path in tables.items()}
            bronze, silver, quarantine, gold = (frames[name] for name in ("bronze", "silver", "quarantine", "gold"))
            input_rows = bronze.count()
            unique_rows = silver.count()
            quarantined = quarantine.count()
            classified = frames["classified"]
            valid_clean = classified.filter("validation_reason IS NULL")
            conflict_ids = frames["conflicting_ids"]
            duplicate_rows = (valid_clean.join(conflict_ids, "event_id", "left_anti").count()
                              - unique_rows)
            if input_rows != unique_rows + duplicate_rows + quarantined:
                raise RuntimeError("input reconciliation failed")
            top_groups = []
            for row in gold.orderBy("event_count", ascending=False).limit(20).collect():
                group = row.asDict(recursive=True)
                for key, value in group.items():
                    if hasattr(value, "isoformat"):
                        group[key] = value.isoformat()
                    elif key == "amount_total_brl":
                        group[key] = str(value)
                top_groups.append(group)
            summary_path = self.run_dir / "dashboard-summary.json"
            self._atomic_json(summary_path, {"label_is_synthetic": True,
                                             "top_groups": top_groups,
                                             "gold_group_count": gold.count()})
            manifest: dict[str, Any] = {
                "manifest_version": 1, "run_id": self.run_id, "status": "completed",
                "schema_version": "payment_event_v1", "pipeline_version": "1.0.0",
                "label_is_synthetic": True,
                "input": {"path": str(self.input_path), "sha256": input_hash, "rows": input_rows,
                          "snapshot_path": str(snapshot), "snapshot_bytes": input_bytes},
                "counts": {"bronze": input_rows, "silver_unique": unique_rows,
                           "duplicates_removed": duplicate_rows, "quarantine": quarantined,
                           "gold": gold.count()},
                "reconciliation": {"formula": "input = unique_valid + duplicates + quarantine", "passed": True},
                "outputs": {name: str(path.relative_to(self.data_root)) for name, path in tables.items()},
                "dashboard": {"gold_path": str(tables["gold"].relative_to(self.data_root)),
                              "summary_path": str(summary_path.relative_to(self.data_root)),
                              "summary": {"gold_rows": gold.count(), "top_groups": top_groups}},
                "started_at": started_at, "finished_at": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
            # Every SDP view read the same frozen file. Verify both retained bytes and
            # original source before publishing, never hash a replacement as the input.
            self._verify_input_snapshot(snapshot, input_hash)
            self._atomic_manifest(manifest)
            return manifest
        except BaseException as error:
            if self.run_dir.exists() and not (self.run_dir / "manifest.json").exists():
                self._atomic_manifest({"manifest_version": 1, "run_id": self.run_id,
                                       "status": "failed", "error_type": type(error).__name__})
            raise
        finally:
            if spark is not None:
                spark.stop()
            self.lock_path.unlink(missing_ok=True)

    @contextmanager
    def _open_regular(self, path: Path) -> Iterator[BinaryIO]:
        """Open a contained regular file without following a replaced final symlink."""
        resolved = path.resolve(strict=True)
        if resolved != path or not resolved.is_relative_to(self.data_root):
            raise ValueError("input path changed or escapes data directory")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError("input must be a regular file")
            yield stream

    def _snapshot_input(self) -> tuple[Path, str, int]:
        """Freeze the bytes once with a bounded copy and retain their digest for audit."""
        snapshot = self.run_dir / "input-snapshot.jsonl"
        checksum, size = sha256(), 0
        with self._open_regular(self.input_path) as source, snapshot.open("xb") as target:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                target.write(block)
                checksum.update(block)
                size += len(block)
            target.flush()
            os.fsync(target.fileno())
        snapshot.chmod(0o440)
        return snapshot, checksum.hexdigest(), size

    def _file_digest(self, path: Path) -> str:
        """Hash contained file bytes in bounded blocks without trusting its pathname."""
        checksum = sha256()
        with self._open_regular(path) as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                checksum.update(block)
        return checksum.hexdigest()

    def _verify_input_snapshot(self, snapshot: Path, expected: str) -> None:
        """Reject source replacement or snapshot changes before a completed manifest."""
        if self._file_digest(snapshot) != expected:
            raise RuntimeError("input snapshot changed during processing")
        if self._file_digest(self.input_path) != expected:
            raise RuntimeError("input changed during processing")

    def _invoke(self, mode: str, spec: Path) -> None:
        """Execute the official Spark Declarative Pipelines CLI."""
        result = subprocess.run(["spark-pipelines", mode, "--spec", str(spec)],
                                check=False, capture_output=True, text=True, timeout=3600,
                                cwd=self.run_dir)
        if result.returncode:
            tail = "\n".join((result.stdout + result.stderr).splitlines()[-30:])
            raise RuntimeError(f"spark-pipelines {mode} failed ({result.returncode}): {tail}")

    @staticmethod
    def _table_paths(warehouse: Path) -> dict[str, Path]:
        """Resolve SDP managed table locations without relying on a persistent catalog."""
        tables = {path.name: path for path in warehouse.rglob("*")
                  if path.is_dir() and path.name in {
                      "bronze", "classified", "conflicting_ids", "quarantine", "silver", "gold"}}
        missing = {"bronze", "classified", "conflicting_ids", "quarantine", "silver", "gold"} - tables.keys()
        if missing:
            raise RuntimeError(f"SDP did not materialize expected datasets: {sorted(missing)}")
        return tables

    def _atomic_manifest(self, data: dict[str, Any]) -> None:
        """Replace the run manifest atomically without exposing partial JSON."""
        self._atomic_json(self.run_dir / "manifest.json", data)

    @staticmethod
    def _atomic_json(path: Path, data: dict[str, Any]) -> None:
        """Write a bounded JSON artifact using a same-directory atomic rename."""
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent,
                                         delete=False) as stream:
            json.dump(data, stream, sort_keys=True, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
