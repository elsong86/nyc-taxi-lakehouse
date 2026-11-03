import json
from dataclasses import replace

import pytest
from pyspark.sql import functions as F

from nyc_taxi_lakehouse.benchmark import delta_write_metrics
from nyc_taxi_lakehouse.gold import GOLD_KEYS, affected_rows
from nyc_taxi_lakehouse.gold_experiment import (
    GoldConfig,
    code_hashes,
    load_manifest,
    sample_plan,
    summarize,
)
from nyc_taxi_lakehouse.pipeline_experiment import assert_equivalent
from nyc_taxi_lakehouse.transforms import gold_tables


def valid_trips(spark):
    rows = [
        ("2024-01", "2024-01-15 10:00:00", "10.00", 1.0, 10.0, True),
        ("2024-02", "2024-01-15 10:00:00", "20.00", 3.0, 20.0, True),
        ("2024-02", "2024-01-15 10:00:00", "100.00", 5.0, 30.0, True),
        ("2024-02", "2024-02-15 10:00:00", "5.00", 2.0, 5.0, False),
        ("2024-01", "2024-03-15 10:00:00", "4.00", 1.0, 3.0, True),
        ("2024-03", "2024-01-15 10:00:00", "-5.00", 7.0, 5.0, True),
        ("2024-03", "2024-01-15 10:00:00", "-5.00", 7.0, 5.0, True),
        ("2024-03", "2024-03-15 10:00:00", "7.00", 2.0, 15.0, True),
    ]
    return (
        spark.createDataFrame(
            rows,
            "source_month string, pickup_at string, total_amount string, "
            "trip_distance_miles double, duration_minutes double, airport_trip boolean",
        )
        .withColumn("pickup_at", F.col("pickup_at").cast("timestamp_ntz"))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)"))
        .withColumn("fare_amount", F.col("total_amount"))
        .withColumn("is_refund", F.col("total_amount") < 0)
        .withColumn("pickup_location_id", F.when(F.col("airport_trip"), 132).otherwise(10))
        .withColumn("dropoff_location_id", F.lit(1))
        .withColumn("pickup_borough", F.lit("Queens"))
        .withColumn("pickup_zone", F.lit("Test"))
    )


def merge_groups(current, patch, keys):
    condition = F.lit(True)
    for key in keys:
        condition = condition & F.col(f"target.{key}").eqNullSafe(F.col(f"patch.{key}"))
    untouched = current.alias("target").join(
        patch.select(*keys).alias("patch"), condition, "left_anti"
    )
    return untouched.unionByName(patch)


def test_affected_key_updates_match_full_gold_across_source_and_pickup_months(spark):
    valid = valid_trips(spark)
    before = gold_tables(valid.filter("source_month < '2024-03'"))
    patches = gold_tables(valid, changed=valid.filter("source_month = '2024-03'"))
    expected = gold_tables(valid)
    for name, keys in GOLD_KEYS.items():
        updated = merge_groups(before[name], patches[name], keys)
        assert_equivalent(expected[name], updated, keys=keys)
        replayed = merge_groups(updated, patches[name], keys)
        assert_equivalent(expected[name], replayed, keys=keys)
    january = patches["monthly_fares"].filter("pickup_month = '2024-01'").first()
    assert january.trip_count == 5
    assert float(january.net_total_amount) == 120.0
    assert float(january.average_total_amount) == 24.0
    assert january.refund_record_count == 2
    assert patches["monthly_fares"].filter("pickup_month = '2024-02'").count() == 0


def test_empty_batch_does_not_recompute_any_gold_groups(spark):
    valid = valid_trips(spark)
    patches = gold_tables(valid, changed=valid.limit(0))
    assert all(frame.count() == 0 for frame in patches.values())


def test_affected_keys_use_null_safe_matching(spark):
    current = spark.createDataFrame([(None, 1), ("x", 2), ("y", 3)], "key string, value int")
    changed = spark.createDataFrame([(None,), ("x",)], "key string")
    assert sorted(row.value for row in affected_rows(current, changed, ["key"]).collect()) == [1, 2]


def test_gold_sample_outputs_are_separate_from_production_and_other_samples():
    config = GoldConfig(experiment_id="gold_test", sample_id="full_1")
    assert config.table("monthly_fares") == "nyc_taxi_dev.gold_test_full_1.monthly_fares"
    assert replace(config, sample_id="incremental_1").table("monthly_fares") != config.table(
        "monthly_fares"
    )
    for values in ({"experiment_id": "../gold"}, {"sample_id": "full_4"}, {"catalog": "a.b"}):
        with pytest.raises(ValueError):
            replace(config, **values)
    with pytest.raises(ValueError):
        config.table("trips")
    with pytest.raises(ValueError):
        replace(config, sample_id="unset").schema


def test_gold_manifest_rejects_changed_code(tmp_path, monkeypatch):
    monkeypatch.setattr(GoldConfig, "root", property(lambda self: tmp_path))
    config = GoldConfig(experiment_id="gold_test")
    manifest = {"experiment_id": "gold_test", "plan": sample_plan(), "code_sha256": {}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    assert code_hashes()
    with pytest.raises(ValueError, match="source changed"):
        load_manifest(config)


def test_gold_summary_separates_operation_time_from_whole_job_and_pending_cost():
    samples = [
        {
            "sample_id": f"full_{index}",
            "strategy": "full",
            "job_run_id": str(index),
            "gold_operation_seconds": elapsed,
            "job_duration_seconds": elapsed + 40,
            "stages": [{"table": name, "elapsed_seconds": elapsed / 4} for name in GOLD_KEYS],
            "tables": [{"active_data_bytes": 100, "num_files": 1} for _ in GOLD_KEYS],
        }
        for index, elapsed in enumerate((30, 10, 20), start=1)
    ]
    summary = summarize(samples)[0]
    assert summary["samples"] == 3
    assert summary["median_gold_operation_seconds"] == 20
    assert summary["median_job_duration_seconds"] == 60
    assert summary["median_active_data_bytes"] == 400
    assert summary["median_data_bytes_added"] is None
    assert summary["billing_status"] == "PENDING"
    assert summary["median_job_list_price_compute_estimate_usd"] is None


def test_delta_write_metrics_include_merge_and_following_compaction():
    history = [
        {"version": 0, "operation": "CLONE", "operationMetrics": {}},
        {
            "version": 1,
            "operation": "MERGE",
            "operationMetrics": {
                "numTargetBytesAdded": "100",
                "numTargetBytesRemoved": "0",
                "numTargetFilesAdded": "2",
                "numTargetFilesRemoved": "0",
            },
        },
        {
            "version": 2,
            "operation": "OPTIMIZE",
            "operationMetrics": {
                "numAddedBytes": "90",
                "numRemovedBytes": "140",
                "numAddedFiles": "1",
                "numRemovedFiles": "3",
            },
        },
    ]
    metrics = delta_write_metrics(history, 0, 2)
    assert metrics["status"] == "COMPLETE"
    assert metrics["data_bytes_added"] == 190
    assert metrics["data_files_added"] == 3
    assert metrics["data_bytes_removed"] == 140
    encoded = [{**row, "operationMetrics": json.dumps(row["operationMetrics"])} for row in history]
    assert delta_write_metrics(encoded, 0, 2) == metrics
    with pytest.raises(ValueError, match="history is incomplete"):
        delta_write_metrics([history[-1]], 0, 2)
    del history[-1]["operationMetrics"]["numAddedBytes"]
    missing = delta_write_metrics(history, 0, 2)
    assert missing["status"] == "INCOMPLETE"
    assert missing["data_bytes_added"] is None
