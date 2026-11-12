"""Exact, explainable baseline for conformance with a fictional simulator.

The initial threshold reflects artificial generator ranges, not banking policy
or demonstrated detection of real fraud. Event keys and labels stay external.
"""

from dataclasses import dataclass
from decimal import Decimal


RULE_ID = "amount_threshold_v1"
RULE_VERSION = "1.0.0"
FEATURE_COLUMNS = ("amount_decimal",)
AMOUNT_AT_OR_ABOVE_THRESHOLD = "AMOUNT_AT_OR_ABOVE_THRESHOLD"
AMOUNT_BELOW_THRESHOLD = "AMOUNT_BELOW_THRESHOLD"


def _validate_amount(value: object, name: str) -> None:
    """Apply payment_event_v1 monetary bounds without rounding or coercion."""
    if (not isinstance(value, Decimal) or not value.is_finite()
            or value.as_tuple().exponent != -2
            or not Decimal("0.01") <= value <= Decimal("1000000.00")):
        raise ValueError(
            f"{name} must be a two-place Decimal in [0.01, 1000000.00]"
        )


def _cents(value: Decimal) -> int:
    """Convert a validated positive two-place value without Decimal context loss."""
    return int("".join(str(digit) for digit in value.as_tuple().digits))


@dataclass(frozen=True)
class RuleConfig:
    """Immutable configuration; changing identity/operator/features is unsupported."""

    threshold_brl: Decimal = Decimal("10000.00")
    rule_id: str = RULE_ID
    rule_version: str = RULE_VERSION
    operator: str = ">="
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS

    def __post_init__(self) -> None:
        """Reject incompatible configurations rather than silently ignoring them."""
        _validate_amount(self.threshold_brl, "threshold_brl")
        if self.rule_id != RULE_ID or self.rule_version != RULE_VERSION:
            raise ValueError("Unsupported rule_id or rule_version")
        if self.operator != ">=":
            raise ValueError("operator must be >=")
        if type(self.feature_columns) is not tuple or self.feature_columns != FEATURE_COLUMNS:
            raise ValueError("feature_columns must contain only amount_decimal as a tuple")

    def to_dict(self) -> dict[str, object]:
        """Return a lossless JSON-compatible configuration with no monetary float."""
        return {
            "rule_id": self.rule_id,
            "rule_version": self.rule_version,
            "threshold_brl": str(self.threshold_brl),
            "operator": self.operator,
            "feature_columns": list(self.feature_columns),
        }

    @classmethod
    def from_dict(cls, value: object) -> "RuleConfig":
        """Require an exact wire schema and canonical decimal text."""
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("RuleConfig requires exactly its declared fields")
        threshold = value["threshold_brl"]
        if (not isinstance(threshold, str) or not threshold.isascii()
                or threshold.count(".") != 1):
            raise ValueError("threshold_brl must be a canonical two-place decimal string")
        whole, fraction = threshold.split(".")
        if (not whole.isdigit() or (len(whole) > 1 and whole.startswith("0"))
                or not fraction.isdigit() or len(fraction) != 2):
            raise ValueError("threshold_brl must be a canonical two-place decimal string")
        if value["feature_columns"] != list(FEATURE_COLUMNS):
            raise ValueError("feature_columns must contain only amount_decimal")
        return cls(
            threshold_brl=Decimal(threshold),
            rule_id=value["rule_id"],
            rule_version=value["rule_version"],
            operator=value["operator"],
            feature_columns=FEATURE_COLUMNS,
        )


@dataclass(frozen=True)
class RuleResult:
    """Validated evidence of one decision, excluding keys and evaluation labels."""

    amount_decimal: Decimal
    config: RuleConfig

    def __post_init__(self) -> None:
        """Keep direct result construction as strict as the scorer."""
        _validate_amount(self.amount_decimal, "amount_decimal")
        if type(self.config) is not RuleConfig:
            raise ValueError("config must be RuleConfig")

    @property
    def score_cents(self) -> int:
        """Return exact integer BRL cents; this score is not a probability."""
        return _cents(self.amount_decimal)

    @property
    def predicted_suspicious(self) -> bool:
        """Apply the inclusive configured threshold."""
        return self.score_cents >= _cents(self.config.threshold_brl)

    @property
    def reason_code(self) -> str:
        """Return stable machine-readable evidence for either decision."""
        return (AMOUNT_AT_OR_ABOVE_THRESHOLD if self.predicted_suspicious
                else AMOUNT_BELOW_THRESHOLD)

    @property
    def reason(self) -> str:
        """Explain the threshold comparison in dashboard-ready language."""
        return ("Valor igual ou superior ao limite da regra" if self.predicted_suspicious
                else "Valor inferior ao limite da regra")

    def to_dict(self) -> dict[str, object]:
        """Expose a flat output schema with exact BRL text and versioned evidence."""
        return {
            "rule_id": self.config.rule_id,
            "rule_version": self.config.rule_version,
            "threshold_brl": str(self.config.threshold_brl),
            "operator": self.config.operator,
            "amount_brl": str(self.amount_decimal),
            "score_cents": self.score_cents,
            "predicted_suspicious": self.predicted_suspicious,
            "reason_code": self.reason_code,
            "reason": self.reason,
        }


def score_amount(amount_decimal: Decimal, config: RuleConfig = RuleConfig()) -> RuleResult:
    """Score only the approved amount; callers must project away all other data."""
    return RuleResult(amount_decimal=amount_decimal, config=config)
