# src/pipelines/silver_nordpool.py
#
# Silver layer:
# - Enriches raw bronze observations with timestamp and price fields.
# - Applies data-quality expectations for observability.
# - Retains invalid observations in an append-only quarantine table.
# - Produces one latest valid price per (country, price_timestamp).

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


VALIDATION_RULES = {
    "valid_country": "country IN ('EE', 'FI', 'LT', 'LV')",
    "valid_price_timestamp": (
        "price_timestamp IS NOT NULL AND price_timestamp > 0"
    ),
    "valid_price": (
        "price IS NOT NULL AND price BETWEEN -500 AND 4000"
    ),
    "valid_batch_id": (
        "batch_id IS NOT NULL AND batch_id <> ''"
    ),
    "valid_fetched_at": (
        "fetched_at IS NOT NULL"
    ),
}


VALIDATION_EXPRESSION = " AND ".join(
    f"({rule})"
    for rule in VALIDATION_RULES.values()
)


def quarantine_reason_expression():
    return (
        F.when(
            ~F.expr(VALIDATION_RULES["valid_country"]),
            F.lit("invalid_country"),
        )
        .when(
            ~F.expr(VALIDATION_RULES["valid_price_timestamp"]),
            F.lit("invalid_price_timestamp"),
        )
        .when(
            ~F.expr(VALIDATION_RULES["valid_price"]),
            F.lit("invalid_price"),
        )
        .when(
            ~F.expr(VALIDATION_RULES["valid_batch_id"]),
            F.lit("invalid_batch_id"),
        )
        .when(
            ~F.expr(VALIDATION_RULES["valid_fetched_at"]),
            F.lit("invalid_fetched_at"),
        )
        .otherwise(F.lit(None).cast("string"))
    )


@dp.temporary_view(
    name="stg_nordpool_silver_base",
    comment=(
        "Typed and enriched Nord Pool observations with validation "
        "status for routing to valid silver or quarantine."
    ),
)
@dp.expect_all(VALIDATION_RULES)
def stg_nordpool_silver_base():
    return (
        dp.read_stream("bronze_nordpool_prices")
        .withColumn(
            "fetched_at",
            F.to_timestamp("fetched_at_utc"),
        )
        .withColumn(
            "price_timestamp_utc",
            F.to_timestamp(
                F.from_unixtime(
                    F.col("price_timestamp")
                )
            ),
        )
        .withColumn(
            "price_timestamp_vilnius",
            F.from_utc_timestamp(
                F.col("price_timestamp_utc"),
                "Europe/Vilnius",
            ),
        )
        .withColumn(
            "price_date_vilnius",
            F.to_date("price_timestamp_vilnius"),
        )
        .withColumn(
            "hour",
            F.hour("price_timestamp_vilnius"),
        )
        .withColumn(
            "minute",
            F.minute("price_timestamp_vilnius"),
        )
        .withColumn(
            "price_eur_mwh",
            F.round(
                F.col("price"),
                2,
            ),
        )
        .withColumn(
            "price_eur_kwh",
            F.round(
                F.col("price") / F.lit(1000),
                5,
            ),
        )
        .withColumn(
            "start_time",
            F.date_format(
                F.col("price_timestamp_vilnius"),
                "HH:mm",
            ),
        )
        .withColumn(
            "end_time",
            F.date_format(
                F.col("price_timestamp_vilnius")
                + F.expr("INTERVAL 15 MINUTES"),
                "HH:mm",
            ),
        )
        .withColumn(
            "time_interval",
            F.concat_ws(
                " - ",
                F.col("start_time"),
                F.col("end_time"),
            ),
        )
        .drop(
            "start_time",
            "end_time",
        )
        .withColumn(
            "is_valid",
            F.expr(VALIDATION_EXPRESSION),
        )
        .withColumn(
            "quarantine_reason",
            quarantine_reason_expression(),
        )
    )


@dp.table(
    name="silver_nordpool_prices_quarantine",
    comment=(
        "Append-only invalid Nord Pool observations retained for "
        "investigation, source troubleshooting, and reprocessing."
    ),
)
@dp.expect(
    "quarantine_record_is_invalid",
    "is_valid = false",
)
def silver_nordpool_prices_quarantine():
    return (
        dp.read_stream("stg_nordpool_silver_base")
        .filter(~F.col("is_valid"))
        .select(
            "batch_id",
            "fetched_at_utc",
            "source_api",
            "country",
            "price_timestamp",
            "price",
            "price_eur_mwh",
            "price_eur_kwh",
            "quarantine_reason",
            "request_window_days_back",
            "request_window_days_forward",
        )
    )


@dp.materialized_view(
    name="silver_nordpool_prices",
    comment=(
        "Validated and deduplicated Nord Pool electricity prices. "
        "The latest valid observation is retained for each country "
        "and delivery timestamp."
    ),
)
@dp.expect(
    "silver_record_is_valid",
    "is_valid = true",
)
def silver_nordpool_prices():
    valid_records = (
        dp.read("stg_nordpool_silver_base")
        .filter(F.col("is_valid"))
    )

    latest_record_window = (
        Window
        .partitionBy(
            "country",
            "price_timestamp",
        )
        .orderBy(
            F.col("fetched_at").desc(),
            F.col("batch_id").desc(),
        )
    )

    return (
        valid_records
        .withColumn(
            "_row_number",
            F.row_number().over(
                latest_record_window
            ),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number",
            "is_valid",
            "quarantine_reason",
        )
        .select(
            "country",
            "price_timestamp",
            "price_timestamp_utc",
            "price_timestamp_vilnius",
            "price_date_vilnius",
            "hour",
            "minute",
            "price_eur_mwh",
            "price_eur_kwh",
            "time_interval",
            "fetched_at",
            "batch_id",
            "source_api",
            "request_window_days_back",
            "request_window_days_forward",
        )
    )