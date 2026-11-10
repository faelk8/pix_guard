"""Application orchestration for isolated, immutable Spark pipeline runs."""

from pathlib import Path
from typing import Any


def process_events(input_path: Path, data_root: Path, run_id: str) -> dict[str, Any]:
    """Process JSONL using Spark while keeping driver memory bounded."""
    from pixguard_ai.domain.pipeline import Reconciliation, validate_run_id
    from pixguard_ai.infrastructure.spark.runner import SparkRun

    return SparkRun(input_path, data_root, validate_run_id(run_id)).execute()
