"""Strict configuration, invalid input and leakage-boundary contract tests."""

import ast
from dataclasses import FrozenInstanceError
from decimal import Decimal
import inspect
import json

import pytest

from pixguard_ai.domain import analysis
from pixguard_ai.domain.analysis import RuleConfig, score_amount


@pytest.mark.parametrize("amount", [
    None, True, 10000, 10000.0, "10000.00", {},
    Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), Decimal("-Infinity"),
    Decimal("10000"), Decimal("10000.0"), Decimal("10000.000"),
    Decimal("0.00"), Decimal("-0.01"), Decimal("1000000.01"),
    Decimal("1E+100"), Decimal("0.001"),
])
def test_invalid_amounts_fail_explicitly(amount: object) -> None:
    """Invalid values must raise rather than produce negative predictions."""
    with pytest.raises(ValueError, match="amount_decimal must be a two-place Decimal"):
        score_amount(amount)
    with pytest.raises(ValueError, match="threshold_brl must be a two-place Decimal"):
        RuleConfig(threshold_brl=amount)


@pytest.mark.parametrize("overrides", [
    {"operator": ">"}, {"operator": None}, {"rule_version": "2.0.0"},
    {"rule_version": None}, {"rule_id": "unknown"},
    {"feature_columns": []}, {"feature_columns": ["amount_decimal"]},
    {"feature_columns": ()},
])
def test_incompatible_configuration_fails(overrides: dict[str, object]) -> None:
    """Reject changes to the supported rule semantics and mutable feature sets."""
    with pytest.raises(ValueError):
        RuleConfig(**overrides)


@pytest.mark.parametrize("feature", [
    "ground_truth", "is_suspicious", "scenario_id", "status", "event_id",
    "event_time", "event_ts", "ingested_at", "raw_json", "canonical_payload",
    "account_id_hash", "device_id_hash", "merchant_id", "unknown",
])
def test_unapproved_features_fail(feature: str) -> None:
    """Use an allowlist rather than accepting raw payload or label columns."""
    with pytest.raises(ValueError, match="feature_columns"):
        RuleConfig(feature_columns=("amount_decimal", feature))
    wire = RuleConfig().to_dict()
    wire["feature_columns"] = ["amount_decimal", feature]
    with pytest.raises(ValueError, match="feature_columns"):
        RuleConfig.from_dict(wire)
    with pytest.raises(TypeError):
        score_amount(Decimal("1.00"), **{feature: True})


def test_config_wire_roundtrip_and_immutability() -> None:
    """Serialize only safe primitives and preserve immutable domain values."""
    config = RuleConfig()
    assert RuleConfig.from_dict(json.loads(json.dumps(config.to_dict()))) == config
    with pytest.raises(FrozenInstanceError):
        config.threshold_brl = Decimal("1.00")
    result = score_amount(Decimal("1.00"))
    encoded_result = json.dumps(result.to_dict(), allow_nan=False)
    assert json.loads(encoded_result)["amount_brl"] == "1.00"
    assert json.loads(encoded_result)["score_cents"] == 100
    with pytest.raises(FrozenInstanceError):
        result.amount_decimal = Decimal("2.00")
    wire = config.to_dict()
    wire["feature_columns"].append("ground_truth")
    assert config.feature_columns == ("amount_decimal",)
    with pytest.raises(ValueError, match="config must be RuleConfig"):
        score_amount(Decimal("1.00"), config=None)


@pytest.mark.parametrize("threshold", [
    None, 10000, 10000.0, "NaN", "1e4", "+10000.00", "010000.00",
    "10000.000", "10000.0", " 10000.00", "10000.00\n", "１００００.００",
])
def test_wire_threshold_rejects_noncanonical_values(threshold: object) -> None:
    """Prevent monetary coercion and alternate textual encodings."""
    wire = RuleConfig().to_dict()
    wire["threshold_brl"] = threshold
    with pytest.raises(ValueError):
        RuleConfig.from_dict(wire)


def test_missing_extra_and_wrong_configuration_shapes_fail() -> None:
    """Unknown keys must never be silently dropped from configuration."""
    wire = RuleConfig().to_dict()
    for invalid in (None, [], {}, dict(wire, ground_truth=True),
                    {key: value for key, value in wire.items() if key != "operator"}):
        with pytest.raises(ValueError, match="exactly"):
            RuleConfig.from_dict(invalid)
    with pytest.raises(TypeError):
        RuleConfig(ground_truth=True)


def test_domain_dependencies_and_money_are_pure() -> None:
    """Guard the narrow standard-library boundary and absence of floating money."""
    tree = ast.parse(inspect.getsource(analysis))
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert imports == {"dataclasses", "decimal"}
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))
    assert not any(isinstance(node, ast.Constant) and type(node.value) is float
                   for node in ast.walk(tree))
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                   and node.func.id == "float" for node in ast.walk(tree))
