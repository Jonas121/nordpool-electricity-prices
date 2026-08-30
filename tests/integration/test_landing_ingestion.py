import pytest

from unittest.mock import patch
from ingest_nordpool_landing import run_ingestion


@patch("ingest_nordpool_landing.fetch_elering_prices")
def test_ingestion_appends_across_runs(mock_fetch, spark, tmp_path):
    mock_fetch.return_value = [
        {"country": "LT", "timestamp": 1735689600, "price": 45.0}
    ]
    table_path = str(tmp_path / "landing_test")

    run_ingestion(table_path, days_back=2, days_forward=2)
    run_ingestion(table_path, days_back=2, days_forward=2)

    result = spark.read.format("delta").load(table_path)
    assert result.count() == 2