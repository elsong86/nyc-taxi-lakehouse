from copy import deepcopy

import pytest

from nyc_taxi_lakehouse.layout_experiment import (
    creation_sql,
    layout_queries,
    target_table,
    validate_distance_precision,
)
from nyc_taxi_lakehouse.layout_preflight import (
    ANNUAL,
    annual_counts,
    annual_layout_queries,
    clustering_gate,
    clustering_on_write_evidence,
    exact_audit,
    measure_queries,
    validate_preflight,
)


def test_annual_source_is_pinned_without_changing_q1_default():
    annual = creation_sql("layout_annual_test", "liquid", 7, ANNUAL)
    assert "fy_2024_trips VERSION AS OF 7" in annual
    assert "q1_2024_trips VERSION AS OF 0" in creation_sql("layout_test", "liquid", 0)
    assert "OR REPLACE" not in annual


def test_annual_counts_reject_missing_duplicate_and_empty_months():
    rows = [{"source_month": f"2024-{m:02d}", "rows": m} for m in range(1, 13)]
    assert annual_counts(rows) == 78
    for invalid in (rows[:-1], rows + [rows[0]], [{**r, "rows": 0} for r in rows]):
        with pytest.raises(ValueError):
            annual_counts(invalid)


def test_physical_gate_requires_clustering_tasks_and_actual_rewrites():
    effects = {
        "clustering_tasks_planned": 2,
        "data_files_added": 3,
        "data_files_removed": 4,
        "data_bytes_added": 100,
        "data_bytes_removed": 120,
    }
    assert clustering_gate(effects)
    for field in effects:
        assert not clustering_gate({**effects, field: 0})
    assert not clustering_gate({**effects, "clustering_tasks_planned": None})


def test_exact_audit_rejects_duplicate_multiplicity_difference(monkeypatch):
    answers = iter([[{"differences": 2}], [{"rows": 10}]])
    monkeypatch.setattr(
        "nyc_taxi_lakehouse.layout_preflight.query_rows", lambda *args: next(answers)
    )
    with pytest.raises(ValueError, match="row-multiset audit"):
        exact_audit(None, "source", "target", 10)


def test_clustering_on_write_is_not_mistaken_for_noop_optimize():
    row = {
        "operation": "CREATE TABLE AS SELECT",
        "operationParameters": (
            '{"clusterBy":"["source_month","pickup_at"]",'
            '"clusteringOnWriteStatus":"kdtree triggered"}'
        ),
        "operationMetrics": '{"numFiles":"34","numOutputBytes":"2000"}',
    }
    no_rewrites = {
        "clustering_tasks_planned": 0,
        "data_files_added": 0,
        "data_files_removed": 0,
        "data_bytes_added": 0,
        "data_bytes_removed": 0,
    }
    assert clustering_gate(no_rewrites, [row])
    assert clustering_on_write_evidence([row])["data_files_written"] == 34
    for status in (
        "Reason for skipping: Estimated ingestion size is not within the expected range",
    ):
        skipped = {**row, "operationParameters": {"clusteringOnWriteStatus": status}}
        assert not clustering_gate(no_rewrites, [skipped])
    assert not clustering_gate(no_rewrites, [{**row, "operationMetrics": {}}])


def test_annual_queries_scan_annual_layouts_and_keep_q1_gold_control():
    for layout in ("partitioned", "liquid"):
        queries = annual_layout_queries("layout_annual_test", layout)
        for query in queries:
            if query["name"] == "04_gold_demand":
                assert "q1_2024_hourly_zone_demand" in query["sql"]
                assert "fy_2024_hourly_zone_demand" not in query["sql"]
            else:
                assert target_table("layout_annual_test", layout) in query["sql"]
                assert "q1_2024_trips" not in query["sql"]
        weekly = next(q["sql"] for q in queries if q["name"] == "02_weekly_zone_demand")
        assert "source_month = '2024-01'" in weekly
        assert "'2024-01-08 00:00:00'" in weekly
        monthly = next(q["sql"] for q in queries if q["name"] == "01_monthly_fares")
        assert "avg(CAST(trip_distance_miles AS DECIMAL(28,6)))" in monthly
    assert "DECIMAL(28,6)" not in layout_queries("layout_test", "partitioned")[0]["sql"]


def test_decimal_distance_guard_rejects_lossy_conversion(monkeypatch):
    monkeypatch.setattr(
        "nyc_taxi_lakehouse.layout_experiment.query_rows",
        lambda *args: [{"rows": 100, "non_roundtripping_rows": 1}],
    )
    with pytest.raises(ValueError, match="precision or overflow"):
        validate_distance_precision(None, "annual")


def test_query_preflight_rejects_unverified_changed_or_inconsistent_layouts():
    layouts = [
        {
            "layout": layout,
            "table": target_table("layout_annual_test", layout),
            "version": 0,
            "audit": {"status": "PASSED", "differences": 0, "rows": 100},
            "optimize_effects": {"clustering_tasks_planned": 0},
            "history": [
                {
                    "operation": "CREATE TABLE AS SELECT",
                    "operationParameters": {"clusteringOnWriteStatus": "kdtree triggered"},
                    "operationMetrics": {"numFiles": "2", "numOutputBytes": "100"},
                }
            ],
        }
        for layout in ("partitioned", "liquid")
    ]
    good = {
        "experiment_id": "layout_annual_test",
        "status": "PREFLIGHT_VERIFIED",
        "physical_clustering_status": "VERIFIED",
        "expected_rows": 100,
        "layouts": layouts,
        "target_versions": {item["table"]: 0 for item in layouts},
    }
    validate_preflight(good)
    invalid = deepcopy(good)
    invalid["layouts"][0]["audit"]["differences"] = 2
    with pytest.raises(ValueError, match="audit"):
        validate_preflight(invalid)
    invalid = deepcopy(good)
    invalid["layouts"][1]["version"] = 1
    with pytest.raises(ValueError, match="version"):
        validate_preflight(invalid)
    with pytest.raises(ValueError, match="physical clustering"):
        validate_preflight({**good, "physical_clustering_status": "NOT_VERIFIED"})
    with pytest.raises(ValueError, match="exactly two"):
        validate_preflight({**good, "layouts": [layouts[0], layouts[0]]})
    invalid = deepcopy(good)
    invalid["layouts"][1]["history"] = []
    with pytest.raises(ValueError, match="supporting physical"):
        validate_preflight(invalid)


def test_existing_query_measurements_are_never_overwritten(tmp_path):
    path = tmp_path / "queries.json"
    path.write_text('{"status":"FAILED"}')
    with pytest.raises(ValueError, match="preserve measurements"):
        measure_queries(None, tmp_path / "missing_preflight.json", path)
    assert path.read_text() == '{"status":"FAILED"}'
