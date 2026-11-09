"""Verify the strict canonical event contract at untrusted JSON boundaries."""

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import json

import pytest

from pixguard_ai.application.generate_events import GenerationConfig
from pixguard_ai.domain.events import PaymentEvent, PaymentFeatures
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource


def sample() -> dict[str, object]:
    """Return one JSON-compatible, valid synthetic event."""
    return next(SyntheticEventSource().events(GenerationConfig(count=1))).to_dict()


def test_contract_round_trip_and_label_separation() -> None:
    """Serialization preserves Decimal/UTC types and excludes labels from inputs."""
    payload = json.loads(json.dumps(sample()))
    event = PaymentEvent.from_dict(payload)
    assert event.to_dict() == payload
    assert isinstance(event.features.amount_brl, Decimal)
    assert event.event_time.tzinfo is timezone.utc
    assert event.ingested_at.tzinfo is timezone.utc
    assert set(payload["ground_truth"]) == {"scenario_id", "is_suspicious"}
    assert not {"ground_truth", "scenario_id", "is_suspicious", "label_is_synthetic"} & payload["features"].keys()
    assert event.label_is_synthetic is True


@pytest.mark.parametrize("field,value", [
    ("schema_version", "payment_event_v2"),
    ("schema_version", None),
    ("event_id", "not-a-uuid"),
    ("event_id", "00000000-0000-4000-8000-000000000000"),
    ("event_id", 42),
    ("label_is_synthetic", False),
    ("label_is_synthetic", 1),
    ("event_time", "2025-01-01T00:00:00"),
    ("event_time", "2025-01-01T00:00:00-03:00"),
    ("event_time", "2025-01-01"),
    ("event_time", None),
    ("ingested_at", "2024-01-01T00:00:00Z"),
    ("features", None),
    ("ground_truth", None),
])
def test_invalid_envelope_is_rejected(field: str, value: object) -> None:
    """Do not silently coerce invalid or incompatible event metadata."""
    payload = sample()
    payload[field] = value
    with pytest.raises(ValueError):
        PaymentEvent.from_dict(payload)


@pytest.mark.parametrize("amount", [None, 12.5, True, "NaN", "Infinity", "0.00", "-1.00", "01.00", "1", "1.0", "1.001", "1e3", "1000000.01"])
def test_invalid_monetary_values_are_rejected(amount: object) -> None:
    """Reject nonfinite, float, out-of-range and noncanonical monetary input."""
    payload = sample()
    payload["features"]["amount_brl"] = amount
    with pytest.raises(ValueError):
        PaymentEvent.from_dict(payload)


@pytest.mark.parametrize("field,value", [
    ("account_id_hash", "real-account"), ("device_id_hash", None),
    ("merchant_id", "real merchant"), ("location_region", "BR-SP"),
    ("channel", "unknown"), ("status", "unknown"),
])
def test_invalid_feature_domains_are_rejected(field: str, value: object) -> None:
    """Only synthetic namespaces and declared categorical values are accepted."""
    payload = sample()
    payload["features"][field] = value
    with pytest.raises(ValueError):
        PaymentEvent.from_dict(payload)


@pytest.mark.parametrize("field", ["cpf", "name", "account", "pix_key", "scenario_id", "ground_truth"])
def test_forbidden_or_unknown_fields_are_rejected(field: str) -> None:
    """PII fields and labels cannot be added to the feature contract."""
    payload = sample()
    for location in (payload, payload["features"]):
        candidate = deepcopy(payload)
        destination = candidate if location is payload else candidate["features"]
        destination[field] = "forbidden"
        with pytest.raises(ValueError):
            PaymentEvent.from_dict(candidate)


def test_every_field_is_required() -> None:
    """No required field at any envelope level may be silently omitted."""
    payload = sample()
    for section in (None, "features", "ground_truth"):
        fields = payload if section is None else payload[section]
        for field in fields:
            candidate = deepcopy(payload)
            del (candidate if section is None else candidate[section])[field]
            with pytest.raises(ValueError):
                PaymentEvent.from_dict(candidate)


@pytest.mark.parametrize("labels", [
    {"scenario_id": "normal", "is_suspicious": True},
    {"scenario_id": "high_amount", "is_suspicious": False},
    {"scenario_id": "normal", "is_suspicious": 0},
    {"scenario_id": "other", "is_suspicious": True},
])
def test_inconsistent_labels_are_rejected(labels: dict[str, object]) -> None:
    """Truth labels have a strict and independently validated schema."""
    payload = sample()
    payload["ground_truth"] = labels
    with pytest.raises(ValueError):
        PaymentEvent.from_dict(payload)


def test_direct_domain_construction_validates_types() -> None:
    """Typed Python callers also get validation instead of bypassing the contract."""
    event = PaymentEvent.from_dict(sample())
    fields = event.features.to_dict()
    fields["amount_brl"] = 1.50
    with pytest.raises(ValueError):
        PaymentFeatures(**fields)
    with pytest.raises(ValueError):
        PaymentEvent(
            event.event_id, datetime(2025, 1, 1), event.ingested_at,
            event.features, event.ground_truth,
        )
