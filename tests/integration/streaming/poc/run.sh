#!/bin/sh
# Run from repository root. Cleanup preserves only this project's evidence volumes.
set -eu
compose() {
    docker compose -p pixguard-stream001 -f docker-compose.yml -f tests/integration/streaming/poc/compose.yaml "$@"
}
trap 'compose down' EXIT
compose config --quiet
compose build app
compose up -d --wait kafka
compose run --rm --no-deps -e PIXGUARD_STREAM001_PHASE=publish app pytest tests/integration/streaming/poc -q -s
compose restart kafka
compose up -d --wait kafka
compose run --rm --no-deps -e PIXGUARD_STREAM001_PHASE=replay app pytest tests/integration/streaming/poc -q -s
