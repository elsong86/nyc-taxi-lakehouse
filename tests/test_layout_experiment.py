import pytest

from nyc_taxi_lakehouse.benchmark import delta_write_metrics
from nyc_taxi_lakehouse.layout_experiment import (
    creation_sql,
    layout_queries,
    optimize_effects,
    target_table,
)


def test_layouts_use_version_pinned_source_and_matching_common_properties():
    partitioned = creation_sql("layout_test", "partitioned", 0)
    liquid = creation_sql("layout_test", "liquid", 0)
    for statement in (partitioned, liquid):
        assert "VERSION AS OF 0" in statement
        assert "'delta.targetFileSize'='67108864'" in statement
        assert "'delta.parquet.compression.codec'='zstd'" in statement
        assert "OR REPLACE" not in statement
    assert "PARTITIONED BY (source_month)" in partitioned
    assert "CLUSTER BY (source_month, pickup_at)" in liquid
    for args in (("q1_2024", "liquid"), ("layout_bad;sql", "liquid"), ("layout_test", "bad")):
        with pytest.raises(ValueError):
            target_table(*args)


def test_queries_change_only_silver_target_and_keep_gold_control():
    a = layout_queries("layout_test", "partitioned")
    b = layout_queries("layout_test", "liquid")
    for qa, qb in zip(a, b, strict=True):
        assert qa["template_sha256"] == qb["template_sha256"]
        if qa["name"] == "04_gold_demand":
            assert qa["sql"] == qb["sql"]
            assert "q1_2024_hourly_zone_demand" in qa["sql"]
        else:
            assert qa["sql"] != qb["sql"]
            assert "layout_test_partitioned_trips" in qa["sql"]
            assert "layout_test_liquid_trips" in qb["sql"]


def test_ctas_write_accounting_preserves_missing_removal_metrics():
    result = delta_write_metrics(
        [
            {
                "version": 0,
                "operation": "CREATE TABLE AS SELECT",
                "operationMetrics": '{"numOutputBytes":"100","numFiles":"2"}',
            }
        ],
        -1,
        0,
    )
    assert result["data_bytes_added"] == 100
    assert result["data_files_added"] == 2
    assert result["data_bytes_removed"] is None
    assert result["status"] == "INCOMPLETE"


def test_optimize_distinguishes_metadata_work_from_physical_rewrites():
    metrics = {
        "numFilesAdded": 0,
        "numFilesRemoved": 0,
        "filesAdded": {"totalSize": 0},
        "filesRemoved": {"totalSize": 0},
        "clusteringMetrics": {"numClusteringTasksPlanned": 0},
    }
    effects = optimize_effects([{"metrics": metrics}])
    assert effects["physical_rewrite_observed"] is False
    assert effects["clustering_tasks_planned"] == 0
    assert effects["data_bytes_added"] == 0
    metrics["numFilesAdded"] = 2
    metrics["filesAdded"]["totalSize"] = 100
    metrics["clusteringMetrics"]["numClusteringTasksPlanned"] = 1
    effects = optimize_effects([{"metrics": metrics}])
    assert effects["physical_rewrite_observed"] is True
    assert effects["data_bytes_added"] == 100
    with pytest.raises(ValueError, match="no operation metrics"):
        optimize_effects([])
