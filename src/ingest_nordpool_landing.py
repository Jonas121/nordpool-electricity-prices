from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
)
from pyspark.sql import SparkSession

from nordpool_api import DEFAULT_API_BASE_URL, fetch_elering_prices


LANDING_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("fetched_at_utc", StringType(), False),
    StructField("request_window_days_back", LongType(), False),
    StructField("request_window_days_forward", LongType(), False),
    StructField("source_api", StringType(), False),
    StructField("country", StringType(), False),
    StructField("price_timestamp", LongType(), False),
    StructField("price", DoubleType(), False),
])

SOURCE_API = "elering_nps_price_api"


def build_landing_rows(
    days_back: int,
    days_forward: int,
    base_url: str = DEFAULT_API_BASE_URL,
) -> list[dict]:
    """Pure function: fetch + shape rows. Raises RuntimeError if no records are returned."""
    fetched_at = datetime.now(timezone.utc)
    fetched_at_utc = fetched_at.isoformat()
    batch_id = fetched_at.strftime("%Y%m%dT%H%M%S%fZ")

    records = fetch_elering_prices(
        days_back=days_back,
        days_forward=days_forward,
        base_url=base_url,
    )

    if not records:
        raise RuntimeError(
            "The API returned no records; landing was not written."
        )

    return [
        {
            "batch_id": batch_id,
            "fetched_at_utc": fetched_at_utc,
            "request_window_days_back": days_back,
            "request_window_days_forward": days_forward,
            "source_api": SOURCE_API,
            "country": record["country"],
            "price_timestamp": int(record["timestamp"]),
            "price": float(record["price"]),
        }
        for record in records
    ]


def run_ingestion(
    landing_table: str,
    days_back: int,
    days_forward: int,
    base_url: str = DEFAULT_API_BASE_URL,
) -> None:
    """Impure orchestration: calls build_landing_rows, writes to Delta."""
    rows = build_landing_rows(days_back, days_forward, base_url)

    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    (
        spark.createDataFrame(rows, schema=LANDING_SCHEMA)
        .write
        .format("delta")
        .mode("append")
        .save(landing_table)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing-table", required=True)
    parser.add_argument("--days-back", type=int, default=2)
    parser.add_argument("--days-forward", type=int, default=2)
    parser.add_argument("--api-base-url", default=DEFAULT_API_BASE_URL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_ingestion(
        landing_table=args.landing_table,
        days_back=args.days_back,
        days_forward=args.days_forward,
        base_url=args.api_base_url,
    )


if __name__ == "__main__":
    main()