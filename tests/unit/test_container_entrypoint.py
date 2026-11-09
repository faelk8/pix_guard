"""Exercise the installed CLI and its shared-volume contract inside Docker."""

import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest

from pixguard_ai.interfaces.cli import check_storage


def test_installed_entrypoint_checks_shared_storage() -> None:
    """The built image must install an executable CLI with volume access."""
    result = subprocess.run(
        ["pixguard", "check-storage"], capture_output=True, text=True, check=True,
    )
    assert "/data/pixguard" in result.stdout
    assert os.getuid() != 0
    assert sys.version_info[:2] == (3, 12)


def test_probe_does_not_leave_shared_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Health checks must not pollute the data directories."""
    directory = tmp_path / "storage"
    monkeypatch.setenv("PIXGUARD_DATA_DIR", str(directory))
    check_storage()
    assert list(directory.iterdir()) == []


def test_service_handles_container_stop() -> None:
    """SIGTERM must end the execution shell promptly with a successful exit."""
    process = subprocess.Popen(
        ["pixguard", "idle"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert process.stdout is not None
        assert "Shared storage" in process.stdout.readline()
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)
