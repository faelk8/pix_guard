"""Exercise offline generation, deterministic replay and streaming persistence."""

import fcntl
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import tracemalloc
from typing import Iterator

import pytest

from pixguard_ai.application.generate_events import GenerationConfig, generate_events
from pixguard_ai.domain.events import PaymentEvent
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.infrastructure.synthetic.jsonl import JsonlEventWriter, manifest_path


def run(output: Path, count: int = 100, seed: int = 42) -> dict[str, object]:
    """Run the real generation use case with local isolated test storage."""
    return generate_events(GenerationConfig(seed, count), SyntheticEventSource(), JsonlEventWriter(output))


def test_defaults_and_complete_batch(tmp_path: Path) -> None:
    """The default run publishes exactly 100,000 unique, valid synthetic events."""
    config = GenerationConfig()
    assert (config.count, config.seed) == (100_000, 42)
    output = tmp_path / "events.jsonl"
    manifest = generate_events(config, SyntheticEventSource(), JsonlEventWriter(output))
    identifiers = set()
    with output.open() as stream:
        for line in stream:
            event = PaymentEvent.from_dict(json.loads(line))
            identifiers.add(event.event_id)
    assert len(identifiers) == 100_000
    assert manifest["count_written"] == manifest["count_requested"] == 100_000
    assert manifest["status"] == "completed"
    assert manifest["progress"] == 1.0
    assert sum(manifest["scenario_counts"].values()) == 100_000
    assert all(count > 0 for count in manifest["scenario_counts"].values())
    assert manifest["sha256"] == sha256(output.read_bytes()).hexdigest()
    assert manifest["bytes_written"] == output.stat().st_size
    assert json.loads(manifest_path(output).read_text()) == manifest


def test_replay_replaces_without_duplicates_and_seed_changes_data(tmp_path: Path) -> None:
    """Same config reproduces every byte and IDs; reruns do not append duplicates."""
    output = tmp_path / "events.jsonl"
    run(output, count=257)
    first = output.read_bytes()
    run(output, count=257)
    assert output.read_bytes() == first
    run(output, count=257, seed=43)
    assert output.read_bytes() != first
    assert len(output.read_text().splitlines()) == 257


def test_larger_count_preserves_existing_sequence_prefix() -> None:
    """Extending a batch does not change the IDs, timestamps or values already generated."""
    source = SyntheticEventSource()
    small = list(source.events(GenerationConfig(count=10)))
    larger = list(source.events(GenerationConfig(count=20)))
    assert small == larger[:10]


def test_progress_reports_flushed_records_before_next_event(tmp_path: Path) -> None:
    """The writer consumes lazily and the dashboard sees actual partial-file counts."""
    output = tmp_path / "events.jsonl"
    observed = []

    def source() -> Iterator[PaymentEvent]:
        for index, event in enumerate(SyntheticEventSource().events(GenerationConfig(count=3))):
            manifest = json.loads(manifest_path(output).read_text())
            observed.append(manifest["count_written"])
            assert len(Path(manifest["partial_path"]).read_text().splitlines()) == index
            assert manifest["status"] == "running"
            assert not output.exists()
            yield event

    JsonlEventWriter(output, progress_interval=1).write(source(), GenerationConfig(count=3))
    assert observed == [0, 1, 2]
    assert output.exists()
    assert not output.with_suffix(".jsonl.partial").exists()


def test_streaming_memory_does_not_scale_with_total_records(tmp_path: Path) -> None:
    """A tenfold batch increase stays within a fixed, small working-memory budget."""
    peaks = []
    for count in (1_000, 10_000):
        tracemalloc.start()
        try:
            run(tmp_path / f"memory-{count}.jsonl", count=count)
            peaks.append(tracemalloc.get_traced_memory()[1])
        finally:
            tracemalloc.stop()
    assert max(peaks) < 2_000_000
    assert peaks[1] < peaks[0] + 500_000


@pytest.mark.parametrize("count,seed", [(0, 42), (-1, 42), (True, 42), (1.5, 42), (1, -1), (1, True), (1, "42")])
def test_invalid_configuration_fails_before_generation(count: object, seed: object) -> None:
    """Invalid counts and seeds cannot create a misleading successful manifest."""
    with pytest.raises(ValueError):
        GenerationConfig(count=count, seed=seed)


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_failure_preserves_previous_output_and_reports_real_progress(tmp_path: Path, failure: type[BaseException]) -> None:
    """A failed run is distinct from completion and never overwrites a valid dataset."""
    output = tmp_path / "events.jsonl"
    run(output, count=5)
    previous = output.read_bytes()

    def source() -> Iterator[PaymentEvent]:
        yield next(SyntheticEventSource().events(GenerationConfig(count=1)))
        raise failure("Do not publish arbitrary exception text")

    with pytest.raises(failure):
        JsonlEventWriter(output).write(source(), GenerationConfig(count=5))
    manifest = json.loads(manifest_path(output).read_text())
    assert manifest["status"] == ("interrupted" if failure is KeyboardInterrupt else "failed")
    assert manifest["count_written"] == 1
    assert manifest["progress"] == 0.2
    assert manifest["error"] == failure.__name__
    assert manifest["finished_at"] is not None
    assert output.read_bytes() == previous
    assert len(Path(manifest["partial_path"]).read_text().splitlines()) == 1
    run(output, count=2)
    assert len(output.read_text().splitlines()) == 2


@pytest.mark.parametrize("actual", [1, 3])
def test_source_count_mismatch_cannot_complete(tmp_path: Path, actual: int) -> None:
    """A broken source never claims the requested volume was produced."""
    output = tmp_path / "events.jsonl"
    with pytest.raises(ValueError):
        JsonlEventWriter(output).write(
            SyntheticEventSource().events(GenerationConfig(count=actual)), GenerationConfig(count=2),
        )
    assert json.loads(manifest_path(output).read_text())["status"] == "failed"
    assert not output.exists()


def test_concurrent_writer_is_rejected(tmp_path: Path) -> None:
    """Two generators cannot race on one output and corrupt its manifest."""
    output = tmp_path / "events.jsonl"
    with output.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="already running"):
            run(output)
    assert not manifest_path(output).exists()


def test_cli_configures_generation_and_rejects_invalid_count(tmp_path: Path) -> None:
    """The installed CLI dispatches the use case with explicit seed/count/output."""
    output = tmp_path / "cli.jsonl"
    result = subprocess.run(
        ["pixguard", "generate", "--seed", "7", "--count", "3", "--output", str(output)],
        capture_output=True, text=True, check=True,
    )
    manifest = json.loads(result.stdout)
    assert manifest["seed"] == 7 and manifest["count_written"] == 3
    result = subprocess.run(
        ["pixguard", "generate", "--count", "0", "--output", str(tmp_path / "invalid.jsonl")],
        capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert not (tmp_path / "invalid.jsonl").exists()
