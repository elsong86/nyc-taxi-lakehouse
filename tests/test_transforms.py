from pyspark.sql import functions as F

from nyc_taxi_lakehouse.transforms import bronze_records, gold_tables, silver_records

SCHEMA = (
    "VendorID string, tpep_pickup_datetime string, tpep_dropoff_datetime string, "
    "passenger_count string, trip_distance string, RatecodeID string, PULocationID string, "
    "DOLocationID string, payment_type string, fare_amount string, tip_amount string, "
    "total_amount string"
)


def raw_row(**overrides):
    return {
        "VendorID": "2",
        "tpep_pickup_datetime": "2024-01-15 10:00:00",
        "tpep_dropoff_datetime": "2024-01-15 10:20:00",
        "passenger_count": None,
        "trip_distance": "4.5",
        "RatecodeID": "1.0",
        "PULocationID": "132",
        "DOLocationID": "1",
        "payment_type": "1",
        "fare_amount": "20.00",
        "tip_amount": "5.00",
        "total_amount": "25.00",
        **overrides,
    }


def zones(spark):
    return spark.createDataFrame(
        [(1, "EWR", "Newark Airport"), (132, "Queens", "JFK Airport")],
        "location_id int, borough string, zone string",
    )


def test_quality_reconciliation_preserves_duplicates_and_refunds(spark):
    rows = [
        raw_row(),
        raw_row(),
        raw_row(total_amount="-25.00", fare_amount="-20.00", tip_amount="-5.00"),
        raw_row(tpep_dropoff_datetime="bad timestamp"),
        raw_row(trip_distance="NaN", PULocationID="999"),
    ]
    bronze = bronze_records(
        spark.createDataFrame(rows, SCHEMA), "2024-01", "test.parquet", "run-one"
    )
    silver = silver_records(bronze, zones(spark))
    collected = silver.collect()
    valid = [row for row in collected if not row.quality_errors]
    rejected = [row for row in collected if row.quality_errors]
    assert len(valid) == 3
    assert len(rejected) == 2
    assert len(valid) + len(rejected) == len(rows)
    assert sum(row.is_refund for row in valid) == 1
    assert all(row.duration_minutes == 20 for row in valid)
    assert all(row.rate_code_id == 1 for row in valid)
    assert all(row.passenger_count is None for row in valid)
    assert "missing_or_invalid_dropoff" in rejected[0].quality_errors
    assert set(rejected[1].quality_errors) == {"invalid_distance", "unknown_pickup_location"}
    assert valid[0].record_fingerprint == valid[1].record_fingerprint


def test_gold_counts_and_money_reconcile(spark):
    raw = spark.createDataFrame([raw_row(), raw_row(total_amount="-5.00")], SCHEMA)
    valid = silver_records(
        bronze_records(raw, "2024-01", "test.parquet", "test-run"), zones(spark)
    ).filter(F.size("quality_errors") == 0)
    gold = gold_tables(valid)
    fares = gold["monthly_fares"].first()
    assert fares.trip_count == 2
    assert float(fares.net_total_amount) == 20.0
    assert fares.refund_record_count == 1
    assert gold["hourly_zone_demand"].first().trip_count == 2
    assert gold["airport_activity"].first().trip_count == 2
    assert gold["trip_patterns"].first().average_distance_miles == 4.5


def test_missing_required_source_columns_fail(spark):
    import pytest

    with pytest.raises(ValueError, match="Required source columns"):
        bronze_records(spark.createDataFrame([(1,)], "VendorID int"), "2024-01", "file", "run")


def test_source_column_case_is_canonicalized_without_duplicate_columns(spark):
    raw = spark.createDataFrame([raw_row()], SCHEMA).withColumn("Airport_fee", F.lit(1.75))
    bronze = bronze_records(raw, "2024-01", "test.parquet", "run")
    assert bronze.columns.count("airport_fee") == 1
    assert "Airport_fee" not in bronze.columns
    assert bronze.select("airport_fee").first()[0] == "1.75"
