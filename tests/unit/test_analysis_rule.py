"""Boundary, exact-money and independent-label tests for the synthetic baseline."""

from decimal import Decimal, localcontext
import inspect

import pytest

from pixguard_ai.domain.analysis import RuleConfig, score_amount


@pytest.mark.parametrize(
    ("amount", "cents", "positive", "code"),
    [
        ("9999.99", 999999, False, "AMOUNT_BELOW_THRESHOLD"),
        ("10000.00", 1000000, True, "AMOUNT_AT_OR_ABOVE_THRESHOLD"),
        ("10000.01", 1000001, True, "AMOUNT_AT_OR_ABOVE_THRESHOLD"),
        ("0.01", 1, False, "AMOUNT_BELOW_THRESHOLD"),
        ("1000000.00", 100000000, True, "AMOUNT_AT_OR_ABOVE_THRESHOLD"),
    ],
)
def test_exact_inclusive_decision(amount: str, cents: int, positive: bool, code: str) -> None:
    """Assert literal expectations at boundaries and the full contract range."""
    result = score_amount(Decimal(amount))
    assert result.score_cents == cents
    assert type(result.score_cents) is int
    assert result.predicted_suspicious is positive
    assert result.reason_code == code
    assert result.to_dict()["amount_brl"] == amount


def test_output_schema_and_versioned_evidence() -> None:
    """Check the complete serialized result against a fixed documented example."""
    assert score_amount(Decimal("10000.00")).to_dict() == {
        "rule_id": "amount_threshold_v1",
        "rule_version": "1.0.0",
        "threshold_brl": "10000.00",
        "operator": ">=",
        "amount_brl": "10000.00",
        "score_cents": 1000000,
        "predicted_suspicious": True,
        "reason_code": "AMOUNT_AT_OR_ABOVE_THRESHOLD",
        "reason": "Valor igual ou superior ao limite da regra",
    }
    assert score_amount(Decimal("1.00")).reason == "Valor inferior ao limite da regra"


def test_custom_threshold_is_respected() -> None:
    """Allow a declared exact threshold without changing version/operator semantics."""
    config = RuleConfig(threshold_brl=Decimal("200.01"))
    assert not score_amount(Decimal("200.00"), config).predicted_suspicious
    assert score_amount(Decimal("200.01"), config).predicted_suspicious
    assert score_amount(Decimal("200.01"), config).to_dict()["threshold_brl"] == "200.01"


def test_decimal_context_cannot_round_the_score() -> None:
    """Even a caller with low precision must get exact cents and comparisons."""
    with localcontext() as context:
        context.prec = 2
        result = score_amount(Decimal("9999.99"))
        assert result.score_cents == 999999
        assert result.predicted_suspicious is False
        assert result.to_dict()["amount_brl"] == "9999.99"


def test_labels_swapped_or_removed_do_not_change_predictions() -> None:
    """Project evaluation rows before scoring; labels never reach the function."""
    rows = [
        {"event_id": "a", "amount_decimal": Decimal("25.00"),
         "ground_truth": {"is_suspicious": False, "scenario_id": "normal"}},
        {"event_id": "b", "amount_decimal": Decimal("15000.00"),
         "ground_truth": {"is_suspicious": True, "scenario_id": "high_amount"}},
    ]
    swapped = [dict(rows[0], ground_truth=rows[1]["ground_truth"]),
               dict(rows[1], ground_truth=rows[0]["ground_truth"])]
    removed = [{"amount_decimal": row["amount_decimal"]} for row in rows]
    expected = [score_amount(row["amount_decimal"]).to_dict() for row in rows]
    for variant in (swapped, removed):
        assert [score_amount(row["amount_decimal"]).to_dict() for row in variant] == expected
    assert list(inspect.signature(score_amount).parameters) == ["amount_decimal", "config"]
    with pytest.raises(TypeError):
        score_amount(**rows[0])
