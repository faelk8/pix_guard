"""Command-line interface for isolated local Spark runs."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import shutil
import yaml
import uuid

from pixguard_ai.application.process_events import process_events


def main() -> None:
    """Parse dry-run and process commands for the Docker pipeline service."""
    parser = argparse.ArgumentParser(description="PixGuard synthetic Spark pipeline")
    subcommands = parser.add_subparsers(dest="command", required=True)
    dry = subcommands.add_parser("dry-run")
    dry.add_argument("--input", type=Path, default=Path("/data/pixguard/events.jsonl"))
    run = subcommands.add_parser("process")
    run.add_argument("--input", type=Path, default=Path("/data/pixguard/events.jsonl"))
    run.add_argument("--run-id", default=None)
    args = parser.parse_args()
    if args.command == "dry-run":
        if not args.input.is_file():
            parser.error(f"input does not exist: {args.input}")
        with tempfile.TemporaryDirectory(prefix="pixguard-sdp-dryrun-") as temporary:
            project = Path(temporary)
            shutil.copyfile(Path(__file__).parents[1] / "infrastructure" / "spark" / "transformations.py",
                            project / "transformations.py")
            spec = project / "spark-pipeline.yml"
            spec.write_text(yaml.safe_dump({
                "name": "pixguard_dry_run", "storage": (project / "checkpoints").as_uri(),
                "libraries": [{"glob": {"include": "transformations.py"}}],
                "configuration": {"spark.pixguard.input": str(args.input.resolve()),
                                  "spark.pixguard.run_id": "dry-run",
                                  "spark.sql.shuffle.partitions": "4"},
            }, sort_keys=False), encoding="utf-8")
            command = ["spark-pipelines", "dry-run", "--spec", str(spec)]
            result = subprocess.run(command, check=False, capture_output=True, text=True, cwd=project)
            if result.returncode:
                parser.exit(result.returncode, (result.stdout + result.stderr)[-4000:])
        print(json.dumps({"status": "validated", "input": str(args.input), "mode": "dry-run"}))
        return
    data_root = Path("/data/pixguard").resolve()
    run_id = args.run_id or uuid.uuid4().hex
    try:
        from pixguard_ai.infrastructure.spark.runner import SparkRun

        manifest = process_events(args.input, data_root, run_id, SparkRun)
    except (ValueError, OSError, RuntimeError, FileExistsError) as error:
        parser.exit(2, f"Pipeline failed: {error}\n")
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
