# tests/unit/test_nordpool_api.py
import pytest
from unittest.mock import patch, MagicMock

from src.nordpool_api import fetch_elering_prices


def make_response(payload, status=200):
    mock_resp = MagicMock()
    mock_resp.status_code = status
    mock_resp.json.return_value = payload
    mock_resp.raise_for_status.side_effect = (
        None if status < 400 else Exception("HTTP error")
    )
    return mock_resp


@patch("src.nordpool_api.requests.get")
def test_fetch_elering_prices_parses_valid_response(mock_get):
    mock_get.return_value = make_response({
        "success": True,
        "data": {
            "lt": [
                {"timestamp": 1735689600, "price": 45.32},
                {"timestamp": 1735693200, "price": 40.10},
            ],
            "ee": [
                {"timestamp": 1735689600, "price": 44.00},
            ],
        },
    })

    records = fetch_elering_prices(days_back=2, days_forward=2)

    assert len(records) == 3
    assert records[0]["country"] == "LT"
    assert isinstance(records[0]["price"], float)
    assert isinstance(records[0]["timestamp"], int)


@patch("src.nordpool_api.requests.get")
def test_fetch_elering_prices_raises_on_unsuccessful_response(mock_get):
    mock_get.return_value = make_response({"success": False, "data": {}})

    with pytest.raises(RuntimeError):
        fetch_elering_prices(days_back=2, days_forward=2)


@patch("src.nordpool_api.requests.get")
def test_fetch_elering_prices_handles_empty_country_data(mock_get):
    mock_get.return_value = make_response({"success": True, "data": {}})

    records = fetch_elering_prices(days_back=2, days_forward=2)

    assert records == []


@patch("src.nordpool_api.requests.get")
def test_fetch_elering_prices_skips_malformed_records(mock_get):
    mock_get.return_value = make_response({
        "success": True,
        "data": {
            "lt": [
                {"timestamp": 1735689600, "price": 45.32},
                {"timestamp": "not-a-number", "price": 40.10},  # malformed
            ],
        },
    })

    records = fetch_elering_prices(days_back=2, days_forward=2)

    assert len(records) == 1


@patch("src.nordpool_api.requests.get")
def test_fetch_elering_prices_uses_correct_date_window(mock_get):
    mock_get.return_value = make_response({"success": True, "data": {}})

    fetch_elering_prices(days_back=3, days_forward=1)

    called_kwargs = mock_get.call_args.kwargs
    params = called_kwargs.get("params", {})
    assert "start" in params
    assert "end" in params