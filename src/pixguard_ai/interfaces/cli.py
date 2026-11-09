"""Container entrypoints for local execution and storage checks."""

import argparse
import importlib.util
import os
from pathlib import Path
import signal
import tempfile
import threading

def data_directory() -> Path:
    """Return the configured shared storage directory."""
    return Path(os.environ.get("PIXGUARD_DATA_DIR", "/data/pixguard"))


def check_storage() -> None:
    """Verify read/write access without leaving probe files in shared data."""
    directory = data_directory()
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(mode="w+", dir=directory) as probe:
        probe.write("pixguard-storage-probe")
        probe.seek(0)
        if probe.read() != "pixguard-storage-probe":
            raise OSError("Shared storage probe could not read its contents")
    print(f"Shared storage is readable and writable: {directory}")


def idle() -> None:
    """Keep the service available until Docker requests a graceful stop."""
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stopped.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stopped.set())
    check_storage()
    print("PixGuard execution shell ready; all payment data is synthetic.")
    stopped.wait()


def dashboard() -> None:
    """Launch the UI when available, otherwise run the reserved service shell."""
    module = importlib.util.find_spec("pixguard_ai.interfaces.dashboard")
    if module is None or module.origin is None:
        print("Dashboard pending UI-001; running the reserved container shell.")
        idle()
        return
    os.execvp(
        "streamlit",
        ["streamlit", "run", module.origin, "--server.address=0.0.0.0",
         "--server.port=8501", "--browser.gatherUsageStats=false"],
    )


def main() -> None:
    """Parse container commands and compose concrete generation adapters."""
    parser = argparse.ArgumentParser(description="PixGuard AI container tools")
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in ("idle", "check-storage", "dashboard"):
        subcommands.add_parser(command)
    arguments = parser.parse_args()
    commands = {"idle": idle, "check-storage": check_storage, "dashboard": dashboard}
    commands[arguments.command]()


if __name__ == "__main__":
    main()
