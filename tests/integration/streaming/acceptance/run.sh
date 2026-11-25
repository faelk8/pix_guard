#!/usr/bin/env bash
# Run from the repository root. Never remove volumes belonging to another project.
set -euo pipefail
project="${PIXGUARD_ACCEPTANCE_PROJECT:-pixguard-acceptance-executor}"
compose=(docker compose -p "$project" -f docker-compose.yml -f tests/integration/streaming/acceptance/compose.yaml)
"${compose[@]}" build app
"${compose[@]}" up -d --wait kafka
"${compose[@]}" run --rm --no-deps -e PIXGUARD_STREAM007_ACCEPTANCE=1 app pytest -q tests/integration/streaming/acceptance --durations=10
