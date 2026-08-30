import pytest
from unittest.mock import patch
from ingest_nordpool_landing import build_landing_rows


@patch("ingest_nordpool_landing.fetch_elering_prices")
def test_build_landing_rows_adds_metadata(mock_fetch):
    mock_fetch.return_value = [
        {"country": "LT", "timestamp": 1735689600, "price": 45.0}
    ]
    rows = build_landing_rows(days_back=2, days_forward=2)
    assert rows[0]["batch_id"]
    assert rows[0]["source_api"] == "elering_nps_price_api"


@patch("ingest_nordpool_landing.fetch_elering_prices")
def test_build_landing_rows_raises_on_empty(mock_fetch):
    mock_fetch.return_value = []
    with pytest.raises(RuntimeError):
        build_landing_rows(days_back=2, days_forward=2)