"""Domain validation helpers for captured Kafka snapshots."""

import re

CAPTURE_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def validate_capture_id(capture_id: str) -> str:
    """Validate a capture identifier before it becomes a filesystem path."""
    if not isinstance(capture_id, str) or not CAPTURE_ID.fullmatch(capture_id):
        raise ValueError("invalid capture_id")
    return capture_id


def validate_snapshot_id(snapshot_id: str) -> str:
    """Validate a snapshot identifier before it becomes a filesystem path."""
    if not isinstance(snapshot_id, str) or not SNAPSHOT_ID.fullmatch(snapshot_id):
        raise ValueError("invalid snapshot_id")
    return snapshot_id
