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


VALID_COUNTRIES = ("EE", "FI", "LT", "LV")


VALID_COUNTRY = (
    "country IN ('EE', 'FI', 'LT', 'LV')"
)

VALID_TIMESTAMP = (
    "price_timestamp IS NOT NULL "
    "AND price_timestamp > 0"
)

VALID_PRICE = (
    "price IS NOT NULL "
    "AND price BETWEEN -500 AND 4000"
)

VALID_BATCH_ID = (
    "batch_id IS NOT NULL "
    "AND batch_id <> ''"
)

VALID_FETCHED_AT = (
    "fetched_at IS NOT NULL "
)


ALL_VALID = " AND ".join([
    f"({VALID_COUNTRY})",
    f"({VALID_TIMESTAMP})",
    f"({VALID_PRICE})",
    f"({VALID_BATCH_ID})",
    f"({VALID_FETCHED_AT})",
])


def quarantine_reason():
    return (
        F.when(
            ~F.expr(VALID_COUNTRY),
            F.lit("invalid_country"),
        )
        .when(
            ~F.expr(VALID_TIMESTAMP),
            F.lit("invalid_timestamp"),
        )
        .when(
            ~F.expr(VALID_PRICE),
            F.lit("invalid_price"),
        )
        .when(
            ~F.expr(VALID_BATCH_ID),
            F.lit("invalid_batch_id"),
        )
        .when(
            ~F.expr(VALID_FETCHED_AT),
            F.lit("invalid_fetched_at"),
        )
        .otherwise(
            F.lit(None).cast("string")
        )
    )


@dp.temporary_view(
    name="stg_nordpool_silver_base",
    comment=(
        "Typed and enriched Nord Pool observations with shared "
        "quality expectations."
    ),
)
@dp.expect(
    "valid_country",
    VALID_COUNTRY,
)
@dp.expect(
    "valid_price_timestamp",
    VALID_TIMESTAMP,
)
@dp.expect(
    "valid_price",
    VALID_PRICE,
)
@dp.expect(
    "valid_batch_id",
    VALID_BATCH_ID,
)
@dp.expect(
    "valid_fetched_at",
    VALID_FETCHED_AT,
)
def stg_nordpool_silver_base():
    return (
        dp.read("bronze_nordpool_prices")
        .withColumn(
            "fetched_at",
            F.to_timestamp("fetched_at_utc"),
        )
        .withColumn(
            "price_timestamp_utc",
            F.to_timestamp(
                F.from_unixtime(
                    F.col("price_timestamp"),
                ),
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
            )
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
            "quarantine_reason",
            quarantine_reason(),
        )
    )


@dp.materialized_view(
    name="silver_nordpool_prices_quarantine",
    comment=(
        "Invalid Nord Pool observations retained for investigation "
        "and reprocessing."
    ),
)
def silver_nordpool_prices_quarantine():
    return (
        dp.read("stg_nordpool_silver_base")
        .filter(
            F.col("quarantine_reason").isNotNull()
        )
        .select(
            "batch_id",
            "fetched_at_utc",
            "fetched_at",
            "source_api",
            "country",
            "price_timestamp",
            "price_timestamp_utc",
            "price_timestamp_vilnius",
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
        "Validated and deduplicated current Nord Pool prices. "
        "The latest valid observation wins for each country and "
        "delivery timestamp."
    ),
)
def silver_nordpool_prices():

    valid_records = (
        dp.read("stg_nordpool_silver_base")
        .filter(
            F.col("quarantine_reason").isNull()
        )
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
                latest_record_window,
            ),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number",
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