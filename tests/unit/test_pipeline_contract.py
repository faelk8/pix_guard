"""Unit tests for run safety and count reconciliation."""

from pathlib import Path
from typing import Any, Never

import pytest

from pixguard_ai.application.process_events import process_events
from pixguard_ai.domain.pipeline import Reconciliation, validate_run_id


def test_reconciliation_accounts_for_each_input_row() -> None:
    """Valid rows, removed duplicates and quarantine exhaust the input."""
    assert Reconciliation(10, 7, 2, 1).reconciles
    assert not Reconciliation(10, 7, 1, 1).reconciles


@pytest.mark.parametrize("run_id", ["run-1", "abc_DEF-09"])
def test_run_id_accepts_safe_tokens(run_id: str) -> None:
    """Safe identifier tokens remain unchanged."""
    assert validate_run_id(run_id) == run_id


@pytest.mark.parametrize("run_id", ["../escape", ".", "", "a/b", "x" * 65])
def test_run_id_rejects_path_traversal(run_id: str) -> None:
    """Run identifiers cannot influence directory traversal."""
    with pytest.raises(ValueError):
        validate_run_id(run_id)


def test_processing_use_case_delegates_without_spark() -> None:
    """A plain adapter can execute the use case in the base app without PySpark."""
    calls: list[tuple[Path, Path, str]] = []
    result: dict[str, Any] = {"status": "completed", "run_id": "fake-run"}

    class FakeRun:
        def __init__(self, input_path: Path, data_root: Path, run_id: str) -> None:
            calls.append((input_path, data_root, run_id))

        def execute(self) -> dict[str, Any]:
            return result

    source, root = Path("/data/pixguard/source.jsonl"), Path("/data/pixguard")
    assert process_events(source, root, "fake-run", FakeRun) is result
    assert calls == [(source, root, "fake-run")]


def test_processing_use_case_rejects_id_before_constructing_adapter() -> None:
    """Invalid identifiers cannot trigger adapter side effects."""
    def forbidden_factory(input_path: Path, data_root: Path, run_id: str) -> Never:
        raise AssertionError("adapter should not be constructed")

    with pytest.raises(ValueError):
        process_events(Path("source"), Path("root"), "../escape", forbidden_factory)
