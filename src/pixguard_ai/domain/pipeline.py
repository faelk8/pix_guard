"""Pure contracts and reconciliation rules for offline Spark runs."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Reconciliation:
    """Counts used to prove that every source line has one disposition."""

    input_rows: int
    unique_valid_rows: int
    duplicate_rows: int
    quarantined_rows: int

    @property
    def reconciles(self) -> bool:
        """Return whether classified input rows account for the source."""
        return self.input_rows == self.unique_valid_rows + self.duplicate_rows + self.quarantined_rows


def validate_run_id(run_id: str) -> str:
    """Reject identifiers that could escape the run directory."""
    import re

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run_id):
        raise ValueError("run_id must contain only letters, digits, '_' or '-' (max 64)")
    return run_id
