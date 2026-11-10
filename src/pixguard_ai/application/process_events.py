"""Application orchestration for isolated, immutable Spark pipeline runs."""

from pathlib import Path
from typing import Any, Protocol

from pixguard_ai.domain.pipeline import validate_run_id


class PipelineRun(Protocol):
    """Port for one prepared execution, independent of the processing framework."""

    def execute(self) -> dict[str, Any]:
        """Return the published manifest or raise when processing fails."""
        ...


class PipelineRunFactory(Protocol):
    """Construct an adapter only after the use case validates the identifier."""

    def __call__(self, input_path: Path, data_root: Path, run_id: str) -> PipelineRun:
        """Prepare the execution for the supplied source and destination."""
        ...


def process_events(
    input_path: Path,
    data_root: Path,
    run_id: str,
    run_factory: PipelineRunFactory,
) -> dict[str, Any]:
    """Validate the request and delegate through an application-owned boundary."""
    return run_factory(input_path, data_root, validate_run_id(run_id)).execute()
