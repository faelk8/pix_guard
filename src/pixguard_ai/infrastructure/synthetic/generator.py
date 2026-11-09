"""Offline fictional payment source with deterministic random state and IDs."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from random import Random
from typing import Iterator
from uuid import NAMESPACE_URL, uuid5

from pixguard_ai.application.generate_events import GenerationConfig
from pixguard_ai.domain.events import GroundTruth, PaymentEvent, PaymentFeatures, SCHEMA_VERSION


BASE_TIME = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _synthetic_hash(kind: str, seed: int, number: int) -> str:
    """Hash an explicitly fictional namespace rather than any personal data."""
    return sha256(f"pixguard:synthetic:{kind}:{seed}:{number}".encode()).hexdigest()


class SyntheticEventSource:
    """Generate normal and high-amount examples without network or personal data."""

    def events(self, config: GenerationConfig) -> Iterator[PaymentEvent]:
        """Yield one event at a time with stable timestamps, features and UUIDs."""
        random = Random(config.seed)
        for index in range(config.count):
            suspicious = random.randrange(100) >= 95
            account = random.randrange(2_000)
            cents = random.randint(1_000_000, 5_000_000) if suspicious else random.randint(100, 200_000)
            event_time = BASE_TIME + timedelta(seconds=index)
            yield PaymentEvent(
                event_id=uuid5(NAMESPACE_URL, f"pixguard/{SCHEMA_VERSION}/{config.seed}/{index}"),
                event_time=event_time,
                ingested_at=event_time + timedelta(milliseconds=10),
                features=PaymentFeatures(
                    account_id_hash=_synthetic_hash("account", config.seed, account),
                    device_id_hash=_synthetic_hash("device", config.seed, account),
                    merchant_id=f"merchant_{random.randrange(1, 501):04d}",
                    amount_brl=(Decimal(cents) / 100).quantize(Decimal("0.01")),
                    location_region=f"region_{random.randrange(1, 6):02d}",
                    channel=random.choice(("mobile", "web")),
                    status="declined" if random.randrange(100) < 3 else "completed",
                ),
                ground_truth=GroundTruth(
                    scenario_id="high_amount" if suspicious else "normal",
                    is_suspicious=suspicious,
                ),
            )
