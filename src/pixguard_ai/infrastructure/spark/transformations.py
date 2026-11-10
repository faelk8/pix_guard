"""Declarative Bronze/Silver/Gold definitions for synthetic JSONL batches."""

import json

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T


_SCHEMA = T.StructType([
    T.StructField("schema_version", T.StringType()), T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.StringType()), T.StructField("ingested_at", T.StringType()),
    T.StructField("label_is_synthetic", T.BooleanType()),
    T.StructField("features", T.StructType([
        T.StructField("account_id_hash", T.StringType()), T.StructField("device_id_hash", T.StringType()),
        T.StructField("merchant_id", T.StringType()), T.StructField("amount_brl", T.StringType()),
        T.StructField("location_region", T.StringType()), T.StructField("channel", T.StringType()),
        T.StructField("status", T.StringType()),
    ])),
    T.StructField("ground_truth", T.StructType([
        T.StructField("scenario_id", T.StringType()), T.StructField("is_suspicious", T.BooleanType()),
    ])),
])


def _contract_error(raw_json: str) -> str | None:
    """Validate JSON using the canonical domain contract without driver collection."""
    try:
        from pixguard_ai.domain.events import PaymentEvent

        PaymentEvent.from_dict(json.loads(raw_json))
    except (TypeError, ValueError, json.JSONDecodeError):
        return "invalid_payment_event_v1"
    return None


def _canonical_json(raw_json: str) -> str | None:
    """Normalize valid payload JSON so key order/spacing do not create conflicts."""
    try:
        from pixguard_ai.domain.events import PaymentEvent

        payload = json.loads(raw_json)
        PaymentEvent.from_dict(payload)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _source() -> DataFrame:
    """Build a fresh reference to the input source for a declarative dataset."""
    source = spark.conf.get("spark.pixguard.input")
    source_file = spark.conf.get("spark.pixguard.source_file", source)
    run_id = spark.conf.get("spark.pixguard.run_id")
    return (spark.read.text(source).withColumnRenamed("value", "raw_json")
            .withColumn("run_id", F.lit(run_id)).withColumn("source_file", F.lit(source_file))
            .withColumn("source_ordinal", F.monotonically_increasing_id()))


def _classified() -> DataFrame:
    """Return source lines with decoded fields and strict validation status."""
    base = _source().withColumn("event", F.from_json("raw_json", _SCHEMA))
    parsed = base.select("*", "event.*").drop("event")
    validate_payload = F.udf(_contract_error, T.StringType())
    canonicalize = F.udf(_canonical_json, T.StringType())
    return (parsed.withColumn("validation_reason", validate_payload("raw_json"))
            .withColumn("canonical_payload", canonicalize("raw_json")))


@dp.materialized_view(name="bronze")
def bronze() -> DataFrame:
    """Preserve each input line and source provenance as a Bronze row."""
    return _source()


@dp.materialized_view(name="classified")
def classified() -> DataFrame:
    """Add strict contract validation results to the raw Bronze rows."""
    return _classified()


@dp.materialized_view(name="conflicting_ids")
def conflicting_ids() -> DataFrame:
    """Find valid event identifiers associated with divergent source payloads."""
    valid = _classified().filter("validation_reason IS NULL")
    return (valid.groupBy("event_id").agg(F.countDistinct("canonical_payload").alias("payload_count"))
            .filter("payload_count > 1").select("event_id"))


@dp.materialized_view(name="quarantine")
def quarantine() -> DataFrame:
    """Quarantine invalid records and every member of a conflicting ID group."""
    classified = _classified()
    conflict_ids = (classified.filter("validation_reason IS NULL")
                    .groupBy("event_id").agg(F.countDistinct("canonical_payload").alias("payload_count"))
                    .filter("payload_count > 1").select("event_id"))
    invalid = (classified.filter("validation_reason IS NOT NULL")
               .withColumnRenamed("validation_reason", "quarantine_reason"))
    conflicts = (classified.filter("validation_reason IS NULL").join(conflict_ids, "event_id", "inner")
                 .withColumn("quarantine_reason", F.lit("conflicting_event_id_payload")))
    return invalid.unionByName(conflicts, allowMissingColumns=True)


@dp.materialized_view(name="silver")
def silver() -> DataFrame:
    """Keep one valid row per event ID after excluding conflicting groups."""
    valid = _classified().filter("validation_reason IS NULL")
    conflict_ids = (valid.groupBy("event_id").agg(F.countDistinct("canonical_payload").alias("payload_count"))
                    .filter("payload_count > 1").select("event_id"))
    clean = valid.join(conflict_ids, "event_id", "left_anti")
    return (clean.dropDuplicates(["event_id"]).withColumn("event_ts", F.to_timestamp("event_time"))
            .withColumn("amount_decimal", F.col("features.amount_brl").cast(T.DecimalType(12, 2))))


@dp.materialized_view(name="gold")
def gold() -> DataFrame:
    """Aggregate deterministic daily totals without label fields or float money."""
    valid = _classified().filter("validation_reason IS NULL")
    conflict_ids = (valid.groupBy("event_id").agg(F.countDistinct("canonical_payload").alias("payload_count"))
                    .filter("payload_count > 1").select("event_id"))
    silver = (valid.join(conflict_ids, "event_id", "left_anti").dropDuplicates(["event_id"])
              .withColumn("event_ts", F.to_timestamp("event_time"))
              .withColumn("amount_decimal", F.col("features.amount_brl").cast(T.DecimalType(12, 2))))
    return (silver.withColumn("event_day_utc", F.to_date("event_ts"))
            .groupBy("event_day_utc", F.col("features.merchant_id").alias("merchant_id"),
                     F.col("features.channel").alias("channel"), F.col("features.status").alias("status"))
            .agg(F.count("event_id").alias("event_count"),
                 F.sum("amount_decimal").alias("amount_total_brl")))
