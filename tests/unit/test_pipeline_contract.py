"""Unit tests for run safety and count reconciliation."""

import pytest

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
