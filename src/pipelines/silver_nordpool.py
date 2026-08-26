from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver_nordpool_prices",
    comment=(
        "Validated current Nord Pool price per country and delivery "
        "timestamp. The latest API observation is retained."
    ),
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
    "fetched_at IS NOT NULL",
)
def silver_nordpool_prices():

    bronze_df = dp.read("bronze_nordpool_prices")

    typed_df = (
        bronze_df
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
            "price_hour_vilnius",
            F.hour("price_timestamp_vilnius"),
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
        typed_df
        .withColumn(
            "_row_number",
            F.row_number().over(latest_record_window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
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