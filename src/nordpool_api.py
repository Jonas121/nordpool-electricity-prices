# nordpool_api.py

import requests
from datetime import datetime, timedelta, timezone

DEFAULT_API_BASE_URL = (
    "https://dashboard.elering.ee/api/nps/price"
)

def fetch_elering_prices(
    days_back: int = 2,
    days_forward: int = 2,
    base_url: str = DEFAULT_API_BASE_URL,
):
    now_utc = datetime.now(timezone.utc)

    start_dt = (
        now_utc - timedelta(days=days_back)
    ).strftime("%Y-%m-%d") + "T00:00:00.000Z"

    end_dt = (
        now_utc + timedelta(days=days_forward)
    ).strftime("%Y-%m-%d") + "T00:00:00.000Z"

    response = requests.get(
        base_url,
        params={
            "start": start_dt,
            "end": end_dt,
        },
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    if not payload.get("success"):
        raise RuntimeError("Elering API returned success=false")

    records = []

    for country_code, price_list in payload.get("data", {}).items():
        for record in price_list:
            records.append({
                "country": country_code.upper(),
                "timestamp": int(record["timestamp"]),
                "price": float(record["price"]),
                "ingested_at": now_utc.isoformat(),
            })

    return records