from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_nordpool_daily_summary",
    comment=(
        "Daily Nord Pool price summary by country and local Lithuanian date."
    ),
)
def gold_nordpool_daily_summary():

    return (
        dp.read("silver_nordpool_prices")
        .groupBy(
            "country",
            "price_date_vilnius",
        )
        .agg(
            F.round(
                F.avg("price_eur_mwh"),
                2,
            ).alias("avg_price_eur_mwh"),
            F.round(
                F.max("price_eur_mwh"),
                2,
            ).alias("max_peak_price_eur_mwh"),
            F.round(
                F.min("price_eur_mwh"),
                2,
            ).alias("min_offpeak_price_eur_mwh"),
            F.round(
                F.avg("price_eur_kwh"),
                5,
            ).alias("avg_price_eur_kwh"),
            F.count(
                "price_timestamp",
            ).alias("interval_count"),
        )
    )