"""Composition root for container-only synthetic analysis and comparison."""

import argparse
import json
import os
from pathlib import Path

from pixguard_ai.application.analyze_events import analyze_events, compare_analyses
from pixguard_ai.domain.analysis import RuleConfig
from pixguard_ai.infrastructure.local.analysis_artifacts import AnalysisArtifacts


def main() -> None:
    """Run a frozen evaluation or verify logical equivalence in a fresh container."""
    parser = argparse.ArgumentParser(description="Fixed synthetic simulator conformance; no fraud generalization")
    commands = parser.add_subparsers(dest="command", required=True)
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("--source-run-id", required=True)
    evaluate.add_argument("--analysis-run-id", required=True)
    compare = commands.add_parser("compare")
    compare.add_argument("--left", required=True)
    compare.add_argument("--right", required=True)
    args = parser.parse_args()
    try:
        from pixguard_ai.infrastructure.spark.analysis import SparkAnalysis

        engine = SparkAnalysis(AnalysisArtifacts(Path(os.environ.get("PIXGUARD_DATA_DIR", "/data/pixguard"))))
        if args.command == "evaluate":
            config = RuleConfig()
            result = analyze_events(engine, args.source_run_id, args.analysis_run_id, config)
        else:
            result = compare_analyses(engine, args.left, args.right)
    except (ValueError, OSError, RuntimeError, KeyError, TypeError) as error:
        parser.exit(2, f"Analysis failed: {error}\n")
    print(json.dumps(result, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
