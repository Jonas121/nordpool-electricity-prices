from typing import Tuple

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


# Validation constants (used for both quarantine and expectations)
VALID_COUNTRY = "country IN ('EE', 'FI', 'LT', 'LV')"
VALID_TIMESTAMP = "price_timestamp IS NOT NULL AND price_timestamp > 0"
VALID_PRICE = "price IS NOT NULL AND price BETWEEN -500 AND 4000"
VALID_BATCH_ID = "batch_id IS NOT NULL AND batch_id <> ''"
VALID_FETCHED_AT = "fetched_at IS NOT NULL"

ALL_VALID = " AND ".join([
    f"({VALID_COUNTRY})",
    f"({VALID_TIMESTAMP})",
    f"({VALID_PRICE})",
    f"({VALID_BATCH_ID})",
    f"({VALID_FETCHED_AT})",
])


# Expectation definitions: (condition, message)
# These mirror the quarantine validation logic
EXPECTATIONS: Tuple[Tuple[str, str], ...] = (
    (VALID_COUNTRY, "wrong country code"),
    (VALID_TIMESTAMP, "price_timestamp is null, zero, or negative"),
    (VALID_PRICE, "price is null or outside valid range"),
    (VALID_BATCH_ID, "batch_id is null or empty"),
    (VALID_FETCHED_AT, "fetched_at is null"),
)


def quarantine_reason_column() -> F.Column:
    """
    Return a Column expression that computes the quarantine reason
    based on validation rules.
    """
    return (
        F.when(~F.expr(VALID_COUNTRY), F.lit("invalid_country"))
        .when(~F.expr(VALID_TIMESTAMP), F.lit("invalid_price_timestamp"))
        .when(~F.expr(VALID_PRICE), F.lit("invalid_price"))
        .when(~F.expr(VALID_BATCH_ID), F.lit("invalid_batch_id"))
        .when(~F.expr(VALID_FETCHED_AT), F.lit("invalid_fetched_at"))
        .otherwise(F.lit(None).cast("string"))
    )


def enrich_bronze(df: DataFrame) -> DataFrame:
    """
    Enrich bronze landing records with derived timestamp and price fields.

    Input columns required:
        - country
        - price_timestamp
        - price
        - batch_id
        - fetched_at_utc

    Returns DataFrame with additional columns:
        - fetched_at
        - price_timestamp_utc
        - price_timestamp_vilnius
        - price_date_vilnius
        - hour
        - minute
        - price_eur_mwh
        - price_eur_kwh
        - time_interval
        - quarantine_reason
    """
    return (
        df.withColumn("fetched_at", F.to_timestamp("fetched_at_utc"))
        .withColumn(
            "price_timestamp_utc",
            F.to_timestamp(F.from_unixtime(F.col("price_timestamp"))),
        )
        .withColumn(
            "price_timestamp_vilnius",
            F.from_utc_timestamp(F.col("price_timestamp_utc"), "Europe/Vilnius"),
        )
        .withColumn("price_date_vilnius", F.to_date("price_timestamp_vilnius"))
        .withColumn("hour", F.hour("price_timestamp_vilnius"))
        .withColumn("minute", F.minute("price_timestamp_vilnius"))
        .withColumn("price_eur_mwh", F.round(F.col("price"), 2))
        .withColumn("price_eur_kwh", F.round(F.col("price") / F.lit(1000), 5))
        .withColumn(
            "start_time",
            F.date_format(F.col("price_timestamp_vilnius"), "HH:mm"),
        )
        .withColumn(
            "end_time",
            F.date_format(
                F.col("price_timestamp_vilnius") + F.expr("INTERVAL 15 MINUTES"),
                "HH:mm",
            ),
        )
        .withColumn(
            "time_interval",
            F.concat_ws(" - ", F.col("start_time"), F.col("end_time")),
        )
        .drop("start_time", "end_time")
        .withColumn("quarantine_reason", quarantine_reason_column())
    )


def deduplicate_latest(df: DataFrame) -> DataFrame:
    """
    Deduplicate records keeping the most recent fetch per (country, price_timestamp).

    Uses row_number() window ordered by fetched_at DESC, batch_id DESC.
    """
    window = (
        Window.partitionBy("country", "price_timestamp")
        .orderBy(F.col("fetched_at").desc(), F.col("batch_id").desc())
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def select_valid(df: DataFrame) -> DataFrame:
    """Filter to records with no quarantine reason (all validations passed)."""
    return df.filter(F.col("quarantine_reason").isNull())


def select_invalid(df: DataFrame) -> DataFrame:
    """Filter to records with a quarantine reason (validation failed)."""
    return df.filter(F.col("quarantine_reason").isNotNull())


def apply_expectations(df: DataFrame) -> DataFrame:
    """
    Apply all expectations to the DataFrame and return it.

    Each expectation is registered via df.expect(condition, message).
    Conditions mirror the quarantine validation logic exactly.
    """
    for condition, message in EXPECTATIONS:
        df.expect(condition, message)
    return df