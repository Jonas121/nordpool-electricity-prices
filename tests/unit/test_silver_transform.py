"""Unit tests for silver transformation logic.

Tests pure PySpark functions in pipelines.silver_transform without
Lakeflow decorators or pipeline runtime dependencies.
"""

import pytest

from pyspark.sql import functions as F

from pipelines.silver_transform import (
    VALID_COUNTRY,
    VALID_TIMESTAMP,
    VALID_PRICE,
    VALID_BATCH_ID,
    VALID_FETCHED_AT,
    EXPECTATIONS,
    enrich_bronze,
    deduplicate_latest,
    select_valid,
    select_invalid,
)


class TestValidationConstants:
    """Test that validation constants are properly defined."""

    def test_valid_country_includes_all_expected_countries(self):
        """Ensure all Baltic and Finnish countries are included."""
        assert "EE" in VALID_COUNTRY
        assert "FI" in VALID_COUNTRY
        assert "LT" in VALID_COUNTRY
        assert "LV" in VALID_COUNTRY

    def test_expectations_count_matches_validation_rules(self):
        """Should have one expectation per validation rule."""
        assert len(EXPECTATIONS) == 5

    def test_expectations_are_tuples_of_condition_and_message(self):
        """Each expectation should be (condition: str, message: str)."""
        for expectation in EXPECTATIONS:
            assert isinstance(expectation, tuple)
            assert len(expectation) == 2
            assert isinstance(expectation[0], str)
            assert isinstance(expectation[1], str)


class TestEnrichBronze:
    """Test the enrich_bronze transformation function."""

    def test_enrich_bronze_computes_price_fields(self, spark):
        """Price conversions should be correct."""
        df = spark.createDataFrame(
            [("LT", 1735689600, 45.0, "b1", "2026-08-27T14:00:00")],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        result = enrich_bronze(df).collect()[0]

        assert result["price_eur_mwh"] == 45.0
        assert result["price_eur_kwh"] == 0.045

    def test_enrich_bronze_converts_timestamps(self, spark):
        """Should convert Unix timestamp to Vilnius timezone."""
        df = spark.createDataFrame(
            [("LT", 1735689600, 45.0, "b1", "2026-08-27T14:00:00")],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        result = enrich_bronze(df).collect()[0]

        assert result["price_timestamp_utc"] is not None
        assert result["price_timestamp_vilnius"] is not None
        assert result["price_date_vilnius"] is not None

    def test_enrich_bronze_computes_time_interval(self, spark):
        """Should compute 15-minute interval window."""
        df = spark.createDataFrame(
            [("LT", 1735689600, 45.0, "b1", "2026-08-27T14:00:00")],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        result = enrich_bronze(df).collect()[0]

        assert result["hour"] is not None
        assert result["minute"] is not None
        assert result["time_interval"] is not None
        assert " - " in result["time_interval"]

    def test_enrich_bronze_valid_record_has_no_quarantine(self, spark):
        """Valid records should have null quarantine_reason."""
        df = spark.createDataFrame(
            [("LT", 1735689600, 45.0, "b1", "2026-08-27T14:00:00")],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        result = enrich_bronze(df).collect()[0]

        assert result["quarantine_reason"] is None

    def test_enrich_bronze_invalid_country(self, spark):
        """Invalid country should be flagged."""
        df = spark.createDataFrame(
            [("XX", 1735689600, 45.0, "b1", "2026-08-27T14:00:00")],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        result = enrich_bronze(df).collect()[0]

        assert result["quarantine_reason"] == "invalid_country"

    def test_enrich_bronze_invalid_timestamp(self, spark):
        """Null or zero timestamp should be flagged."""
        df = spark.createDataFrame(
            [("LT", 0, 45.0, "b1", "2026-08-27T14:00:00")],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        result = enrich_bronze(df).collect()[0]

        assert result["quarantine_reason"] == "invalid_price_timestamp"

    def test_enrich_bronze_invalid_price_null(self, spark):
        """Null price should be flagged."""
        df = spark.createDataFrame(
            [("LT", 1735689600, None, "b1", "2026-08-27T14:00:00")],
            "country STRING, price_timestamp LONG, price DOUBLE, batch_id STRING, fetched_at_utc STRING",
        )
        result = enrich_bronze(df).collect()[0]

        assert result["quarantine_reason"] == "invalid_price"

    def test_enrich_bronze_invalid_price_out_of_range(self, spark):
        """Price outside [-500, 4000] should be flagged."""
        df = spark.createDataFrame(
            [("LT", 1735689600, 5000.0, "b1", "2026-08-27T14:00:00")],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        result = enrich_bronze(df).collect()[0]

        assert result["quarantine_reason"] == "invalid_price"

    def test_enrich_bronze_invalid_batch_id(self, spark):
        """Null or empty batch_id should be flagged."""
        df = spark.createDataFrame(
            [("LT", 1735689600, 45.0, None, "2026-08-27T14:00:00")],
            "country STRING, price_timestamp LONG, price DOUBLE, batch_id STRING, fetched_at_utc STRING",
        )
        result = enrich_bronze(df).collect()[0]
        assert result["quarantine_reason"] == "invalid_batch_id"

    def test_enrich_bronze_invalid_fetched_at(self, spark):
        """Null or empty fetched_at_utc should be flagged."""
        df = spark.createDataFrame(
            [("LT", 1735689600, 45.0, "b1", None)],
            "country STRING, price_timestamp LONG, price DOUBLE, batch_id STRING, fetched_at_utc STRING",
        )
        result = enrich_bronze(df).collect()[0]
        assert result["quarantine_reason"] == "invalid_fetched_at"

    def test_enrich_bronze_preserves_all_input_columns(self, spark):
        """All original columns should still exist after enrichment."""
        df = spark.createDataFrame(
            [("LT", 1735689600, 45.0, "b1", "2026-08-27T14:00:00")],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        result = enrich_bronze(df)

        original_cols = {"country", "price_timestamp", "price", "batch_id", "fetched_at_utc"}
        output_cols = set(result.columns)

        assert original_cols.issubset(output_cols)


class TestDeduplicateLatest:
    """Test the deduplicate_latest function."""

    def test_deduplicate_latest_keeps_most_recent_fetch(self, spark):
        """Should keep the row with the latest fetched_at."""
        df = spark.createDataFrame(
            [
                ("LT", 1735689600, "2026-08-25T14:00:00", "b1"),
                ("LT", 1735689600, "2026-08-26T14:00:00", "b2"),
            ],
            ["country", "price_timestamp", "fetched_at", "batch_id"],
        ).withColumn("fetched_at", F.to_timestamp("fetched_at"))

        result = deduplicate_latest(df).collect()

        assert len(result) == 1
        assert result[0]["batch_id"] == "b2"

    def test_deduplicate_latest_handles_multiple_countries(self, spark):
        """Deduplication should be per (country, price_timestamp)."""
        df = spark.createDataFrame(
            [
                ("LT", 1735689600, "2026-08-25T14:00:00", "b1"),
                ("LT", 1735689600, "2026-08-26T14:00:00", "b2"),
                ("EE", 1735689600, "2026-08-25T14:00:00", "b3"),
                ("EE", 1735689600, "2026-08-26T14:00:00", "b4"),
            ],
            ["country", "price_timestamp", "fetched_at", "batch_id"],
        ).withColumn("fetched_at", F.to_timestamp("fetched_at"))

        result = deduplicate_latest(df).collect()

        assert len(result) == 2
        batch_ids = {row["batch_id"] for row in result}
        assert batch_ids == {"b2", "b4"}

    def test_deduplicate_latest_uses_batch_id_as_tiebreaker(self, spark):
        """When fetched_at is equal, should use batch_id DESC."""
        df = spark.createDataFrame(
            [
                ("LT", 1735689600, "2026-08-26T14:00:00", "b1"),
                ("LT", 1735689600, "2026-08-26T14:00:00", "b2"),
            ],
            ["country", "price_timestamp", "fetched_at", "batch_id"],
        ).withColumn("fetched_at", F.to_timestamp("fetched_at"))

        result = deduplicate_latest(df).collect()

        assert len(result) == 1
        assert result[0]["batch_id"] == "b2"

    def test_deduplicate_latest_removes_row_number_column(self, spark):
        """Internal _rn column should be dropped."""
        df = spark.createDataFrame(
            [
                ("LT", 1735689600, "2026-08-25T14:00:00", "b1"),
                ("LT", 1735689600, "2026-08-26T14:00:00", "b2"),
            ],
            ["country", "price_timestamp", "fetched_at", "batch_id"],
        ).withColumn("fetched_at", F.to_timestamp("fetched_at"))

        result = deduplicate_latest(df)

        assert "_rn" not in result.columns


class TestSelectValidAndInvalid:
    """Test the select_valid and select_invalid filter functions."""

    def test_select_valid_and_invalid_partition_correctly(self, spark):
        """Valid and invalid records should be correctly separated."""
        df = spark.createDataFrame(
            [
                ("LT", 1735689600, 45.0, "b1", "2026-08-27T14:00:00"),
                ("XX", 1735689600, 45.0, "b1", "2026-08-27T14:00:00"),
            ],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        enriched = enrich_bronze(df)

        valid = select_valid(enriched).collect()
        invalid = select_invalid(enriched).collect()

        assert len(valid) == 1
        assert len(invalid) == 1
        assert valid[0]["country"] == "LT"
        assert invalid[0]["country"] == "XX"

    def test_select_valid_returns_only_null_quarantine(self, spark):
        """select_valid should return only rows with null quarantine_reason."""
        df = spark.createDataFrame(
            [
                ("LT", 1735689600, 45.0, "b1", "2026-08-27T14:00:00"),
                ("EE", 1735689600, 5000.0, "b1", "2026-08-27T14:00:00"),  # invalid price
            ],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        enriched = enrich_bronze(df)
        valid = select_valid(enriched).collect()

        assert len(valid) == 1
        assert valid[0]["quarantine_reason"] is None

    def test_select_invalid_returns_only_non_null_quarantine(self, spark):
        """select_invalid should return only rows with non-null quarantine_reason."""
        df = spark.createDataFrame(
            [
                ("LT", 1735689600, 45.0, "b1", "2026-08-27T14:00:00"),
                ("EE", 1735689600, 5000.0, "b1", "2026-08-27T14:00:00"),  # invalid price
            ],
            ["country", "price_timestamp", "price", "batch_id", "fetched_at_utc"],
        )
        enriched = enrich_bronze(df)
        invalid = select_invalid(enriched).collect()

        assert len(invalid) == 1
        assert invalid[0]["quarantine_reason"] is not None

    def test_select_empty_dataframe(self, spark):
        """select_valid on empty DataFrame should return empty."""
        df = spark.createDataFrame(
            [],
            "country STRING, price_timestamp LONG, price DOUBLE, batch_id STRING, fetched_at_utc STRING, quarantine_reason STRING",
        )
        result = select_valid(df)
        assert result.count() == 0
