"""Canonical, strict payment_event_v1 contract for fictional payment events.

Owner: PixGuard ingestion. Granularity: one synthetic payment, keyed by event_id.
All fields are required and non-null; unknown fields and versions are rejected.
Money uses BRL Decimal with two fractional digits, serialized as a JSON string.
Times are UTC; hashes identify generated entities, never real account holders.
Breaking changes require a new schema version. Labels stay outside features.
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import re
from typing import Mapping, cast
from uuid import UUID


SCHEMA_VERSION = "payment_event_v1"
SCENARIOS = ("normal", "high_amount")


def _fields(value: object, expected: set[str], name: str) -> Mapping[str, object]:
    """Reject missing/unknown fields rather than silently dropping input data."""
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"{name} must contain exactly {sorted(expected)}")
    return value


def _utc(value: datetime, name: str) -> None:
    """Require explicit UTC rather than assuming a timezone for naive values."""
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be a timezone-aware UTC datetime")


def _timestamp(value: object, name: str) -> datetime:
    """Parse a timestamp without accepting Python's permissive date formats."""
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", value,
    ):
        raise ValueError(f"{name} must be an ISO 8601 UTC timestamp")
    parsed = datetime.fromisoformat(value)
    _utc(parsed, name)
    return parsed


@dataclass(frozen=True)
class PaymentFeatures:
    """Prediction inputs, excluding simulation labels and scenario metadata."""

    account_id_hash: str
    device_id_hash: str
    merchant_id: str
    amount_brl: Decimal
    location_region: str
    channel: str
    status: str

    def __post_init__(self) -> None:
        """Validate the complete, non-null feature set at construction."""
        for name in ("account_id_hash", "device_id_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
                raise ValueError(f"{name} must be a synthetic SHA-256 identifier")
        if not isinstance(self.merchant_id, str) or not re.fullmatch(
            r"merchant_\d{4}", self.merchant_id,
        ):
            raise ValueError("merchant_id must be a fictional merchant_NNNN")
        amount = self.amount_brl
        if (not isinstance(amount, Decimal) or not amount.is_finite()
                or amount.as_tuple().exponent != -2
                or not Decimal("0.01") <= amount <= Decimal("1000000.00")):
            raise ValueError("amount_brl must be a two-place Decimal in [0.01, 1000000.00]")
        if self.location_region not in tuple(f"region_{index:02d}" for index in range(1, 6)):
            raise ValueError("location_region must be fictional region_01 through region_05")
        if self.channel not in ("mobile", "web"):
            raise ValueError("channel must be mobile or web")
        if self.status not in ("completed", "declined"):
            raise ValueError("status must be completed or declined")

    def to_dict(self) -> dict[str, str]:
        """Return only approved prediction inputs with lossless BRL encoding."""
        return {
            "account_id_hash": self.account_id_hash,
            "device_id_hash": self.device_id_hash,
            "merchant_id": self.merchant_id,
            "amount_brl": str(self.amount_brl),
            "location_region": self.location_region,
            "channel": self.channel,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, value: object) -> "PaymentFeatures":
        """Reject coercions, extra fields and noncanonical monetary values."""
        fields = _fields(value, set(cls.__dataclass_fields__), "features")
        if not all(isinstance(item, str) for item in fields.values()):
            raise ValueError("All wire features must be strings")
        strings = cast(Mapping[str, str], fields)
        amount = strings["amount_brl"]
        if not re.fullmatch(r"(?:0|[1-9]\d*)\.\d{2}", amount):
            raise ValueError("amount_brl must be a decimal string with two fractional digits")
        return cls(
            account_id_hash=strings["account_id_hash"],
            device_id_hash=strings["device_id_hash"],
            merchant_id=strings["merchant_id"],
            amount_brl=Decimal(amount),
            location_region=strings["location_region"],
            channel=strings["channel"],
            status=strings["status"],
        )


@dataclass(frozen=True)
class GroundTruth:
    """Simulation-only labels; never use this object as a prediction feature."""

    scenario_id: str
    is_suspicious: bool

    def __post_init__(self) -> None:
        """Validate known scenarios and the consistency of their synthetic label."""
        if self.scenario_id not in SCENARIOS or type(self.is_suspicious) is not bool:
            raise ValueError("Unknown scenario or non-boolean synthetic label")
        if self.is_suspicious != (self.scenario_id != "normal"):
            raise ValueError("Synthetic label does not match scenario")

    @classmethod
    def from_dict(cls, value: object) -> "GroundTruth":
        """Validate labels separately from prediction inputs."""
        fields = _fields(value, set(cls.__dataclass_fields__), "ground_truth")
        scenario, suspicious = fields["scenario_id"], fields["is_suspicious"]
        if not isinstance(scenario, str) or not isinstance(suspicious, bool):
            raise ValueError("Ground truth requires a scenario string and boolean label")
        return cls(scenario_id=scenario, is_suspicious=suspicious)


@dataclass(frozen=True)
class PaymentEvent:
    """Validated versioned envelope with separate features and ground truth."""

    event_id: UUID
    event_time: datetime
    ingested_at: datetime
    features: PaymentFeatures
    ground_truth: GroundTruth
    schema_version: str = SCHEMA_VERSION
    label_is_synthetic: bool = True

    def __post_init__(self) -> None:
        """Reject invalid metadata and any event not explicitly synthetic."""
        if self.schema_version != SCHEMA_VERSION or self.label_is_synthetic is not True:
            raise ValueError("Only synthetic payment_event_v1 events are supported")
        if not isinstance(self.event_id, UUID) or self.event_id.version != 5:
            raise ValueError("event_id must be a deterministic UUID v5")
        _utc(self.event_time, "event_time")
        _utc(self.ingested_at, "ingested_at")
        if self.ingested_at < self.event_time:
            raise ValueError("ingested_at cannot precede event_time")
        if not isinstance(self.features, PaymentFeatures):
            raise ValueError("features must be PaymentFeatures")
        if not isinstance(self.ground_truth, GroundTruth):
            raise ValueError("ground_truth must be GroundTruth")

    def to_dict(self) -> dict[str, object]:
        """Serialize with UTC Z timestamps and no monetary float conversion."""
        return {
            "schema_version": self.schema_version,
            "event_id": str(self.event_id),
            "event_time": self.event_time.isoformat().replace("+00:00", "Z"),
            "ingested_at": self.ingested_at.isoformat().replace("+00:00", "Z"),
            "label_is_synthetic": self.label_is_synthetic,
            "features": self.features.to_dict(),
            "ground_truth": asdict(self.ground_truth),
        }

    @classmethod
    def from_dict(cls, value: object) -> "PaymentEvent":
        """Validate untrusted JSON objects using the same canonical contract."""
        fields = _fields(value, set(cls.__dataclass_fields__), "event")
        identifier = fields["event_id"]
        if not isinstance(identifier, str):
            raise ValueError("event_id must be a UUID string")
        event_id = UUID(identifier)
        if str(event_id) != identifier:
            raise ValueError("event_id must use the canonical lowercase UUID format")
        version, synthetic = fields["schema_version"], fields["label_is_synthetic"]
        if not isinstance(version, str) or not isinstance(synthetic, bool):
            raise ValueError("Schema version must be a string and synthetic flag a boolean")
        return cls(
            event_id=event_id,
            event_time=_timestamp(fields["event_time"], "event_time"),
            ingested_at=_timestamp(fields["ingested_at"], "ingested_at"),
            features=PaymentFeatures.from_dict(fields["features"]),
            ground_truth=GroundTruth.from_dict(fields["ground_truth"]),
            schema_version=version,
            label_is_synthetic=synthetic,
        )
