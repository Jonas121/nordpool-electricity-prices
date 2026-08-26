from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(
    name="stg_silver_nordpool",
    comment="Validated and typed Nord Pool observations from bronze.",
)
@dp.expect_or_drop(
    "valid_country",
    "country IN ('EE', 'FI', 'LT', 'LV')",
)
@dp.expect_or_drop(
    "valid_price_timestamp",
    "price_timestamp IS NOT NULL AND price_timestamp > 0",
)
@dp.expect_or_drop(
    "valid_price",
    "price IS NOT NULL AND price BETWEEN -500 AND 4000",
)
@dp.expect_or_drop(
    "valid_batch_id",
    "batch_id IS NOT NULL AND batch_id <> ''",
)
@dp.expect_or_drop(
    "valid_fetched_at",
    "fetched_at_utc IS NOT NULL AND fetched_at_utc <> ''",
)
def stg_silver_nordpool():
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
            "price_hour_vilnius",
            F.hour("price_timestamp_vilnius"),
        )
        .select(
            "country",
            "price_timestamp",
            "price_timestamp_utc",
            "price_timestamp_vilnius",
            "price_date_vilnius",
            "price_hour_vilnius",
            "price",
            "fetched_at",
            "batch_id",
            "source_api",
            "request_window_days_back",
            "request_window_days_forward",
        )
    )


dp.create_streaming_table(
    name="silver_nordpool_prices",
    comment=(
        "Latest valid price per country and price timestamp. "
        "Later API observations replace earlier observations."
    ),
)


dp.create_auto_cdc_flow(
    target="silver_nordpool_prices",
    source="stg_silver_nordpool",
    keys=[
        "country",
        "price_timestamp",
    ],
    sequence_by=F.struct(
        F.col("fetched_at"),
        F.col("batch_id"),
    ),
    stored_as_scd_type=1,
)