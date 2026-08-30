# src/pipelines/silver_nordpool.py
#
# Silver layer:
# - Enriches raw bronze observations with timestamp and price fields.
# - Applies data-quality expectations for observability.
# - Retains invalid observations in an append-only quarantine table.
# - Produces one latest valid price per (country, price_timestamp).

from pyspark import pipelines as dp

from pipelines.silver_transform import (
    enrich_bronze,
    deduplicate_latest,
    select_valid,
    select_invalid,
    apply_expectations,
)


@dp.temporary_view(name="stg_nordpool_silver_base")
def stg_nordpool_silver_base():
    """
    Temporary staging view: enrich bronze with derived fields and quarantine flags.

    Applies expectations defined in silver_transform.py.
    """
    base = enrich_bronze(dp.read("bronze_nordpool_prices"))
    base = apply_expectations(base)
    return base


@dp.materialized_view(name="silver_nordpool_prices_quarantine")
def silver_nordpool_prices_quarantine():
    """
    Materialized quarantine view: records that failed validation checks.
    """
    return select_invalid(dp.read("stg_nordpool_silver_base"))


@dp.materialized_view(name="silver_nordpool_prices")
def silver_nordpool_prices():
    """
    Materialized silver view: valid, deduplicated price records.
    """
    return deduplicate_latest(select_valid(dp.read("stg_nordpool_silver_base")))