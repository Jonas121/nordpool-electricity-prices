from pyspark import pipelines as dp
from pyspark.sql import functions as F


LANDING_TABLE = spark.conf.get(
    "nordpool.landing_table",
)


@dp.temporary_view(
    name="stg_nordpool_landing",
    comment="New records from the persistent Nord Pool API landing table.",
)
def stg_nordpool_landing():
    return (
        spark.readStream
        .table(LANDING_TABLE)
        .select(
            F.col("batch_id").cast("string"),
            F.col("fetched_at_utc").cast("string"),
            F.col("request_window_days_back").cast("long"),
            F.col("request_window_days_forward").cast("long"),
            F.col("source_api").cast("string"),
            F.col("country").cast("string"),
            F.col("price_timestamp").cast("long"),
            F.col("price").cast("double"),
        )
    )


@dp.table(
    name="bronze_nordpool_prices",
    comment=(
        "Append-only raw Nord Pool/Elering observations. "
        "Every daily API batch is retained, including overlapping records."
    ),
)
def bronze_nordpool_prices():
    return dp.read_stream("stg_nordpool_landing")