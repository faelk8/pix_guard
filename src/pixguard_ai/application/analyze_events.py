"""Use cases for immutable simulator conformance evaluations."""

from typing import Any, Protocol

from pixguard_ai.domain.analysis import RuleConfig
from pixguard_ai.domain.pipeline import validate_run_id


class AnalysisEngine(Protocol):
    """Execution boundary implemented by the containerized Spark adapter."""

    def evaluate(self, source_run_id: str, analysis_run_id: str,
                 config: RuleConfig) -> dict[str, Any]:
        """Publish one complete evaluation, or preserve a failed attempt."""
        ...

    def compare(self, left: str, right: str) -> dict[str, Any]:
        """Validate and compare two immutable evaluations."""
        ...


def analyze_events(engine: AnalysisEngine, source_run_id: str,
                   analysis_run_id: str, config: RuleConfig) -> dict[str, Any]:
    """Validate identifiers and delegate execution without framework dependencies."""
    return engine.evaluate(validate_run_id(source_run_id),
                           validate_run_id(analysis_run_id), config)


def compare_analyses(engine: AnalysisEngine, left: str, right: str) -> dict[str, Any]:
    """Compare logical contents, independently of artifact encoding and run IDs."""
    return engine.compare(validate_run_id(left), validate_run_id(right))
