"""Generation use case and ports, independent of storage and randomness adapters."""

from dataclasses import dataclass
from typing import Iterable, Protocol, TypedDict

from pixguard_ai.domain.events import PaymentEvent


class GenerationManifest(TypedDict):
    """Shared dashboard snapshot; counters describe the current generation run."""

    manifest_version: int
    schema_version: str
    label_is_synthetic: bool
    status: str
    seed: int
    count_requested: int
    count_written: int
    progress: float
    output_path: str
    partial_path: str
    started_at: str
    updated_at: str
    finished_at: str | None
    elapsed_seconds: float
    scenario_counts: dict[str, int]
    bytes_written: int
    sha256: str
    error: str | None


@dataclass(frozen=True)
class GenerationConfig:
    """Reproducible generation parameters; no wall clock participates in events."""

    seed: int = 42
    count: int = 100_000

    def __post_init__(self) -> None:
        """Reject invalid requests before creating any output artifacts."""
        if type(self.seed) is not int or self.seed < 0:
            raise ValueError("seed must be a nonnegative integer")
        if type(self.count) is not int or self.count < 1:
            raise ValueError("count must be a positive integer")


class EventSource(Protocol):
    """Port for a lazy, validated event source."""

    def events(self, config: GenerationConfig) -> Iterable[PaymentEvent]:
        """Yield the requested events without collecting the dataset."""
        ...


class EventWriter(Protocol):
    """Port for streaming persistence and truthful progress reporting."""

    def write(self, events: Iterable[PaymentEvent], config: GenerationConfig) -> GenerationManifest:
        """Persist events and return the completed generation manifest."""
        ...


def generate_events(
    config: GenerationConfig, source: EventSource, writer: EventWriter,
) -> GenerationManifest:
    """Connect lazy generation to persistence through application-owned ports."""
    return writer.write(source.events(config), config)
