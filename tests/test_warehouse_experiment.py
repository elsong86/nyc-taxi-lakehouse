from decimal import Decimal
from types import SimpleNamespace

import pytest

from nyc_taxi_lakehouse.warehouse_experiment import (
    billing_totals,
    measure_sample,
    sample_plan,
    timed_queries,
    warehouse_name,
)


def test_fresh_sample_names_are_distinct_and_reject_unsafe_ids():
    names = [warehouse_name("sql_clean_test", p["sample_id"]) for p in sample_plan()]
    assert len(set(names)) == 6
    for experiment_id, sample_id in (("bad/id", "2xs_1"), ("sql_clean_test", "unknown")):
        with pytest.raises(ValueError):
            warehouse_name(experiment_id, sample_id)


def test_timed_queries_exclude_warmups_and_complete_balanced_cycles(monkeypatch):
    clock = [0.0]
    calls = []

    class Cursor:
        description = [("count",)]
        query_id = ""

        def execute(self, statement):
            calls.append(statement)
            self.query_id = str(len(calls))
            clock[0] += 1

        def fetchall(self):
            return [(100,)]

    monkeypatch.setattr(
        "nyc_taxi_lakehouse.warehouse_experiment.time.perf_counter", lambda: clock[0]
    )
    queries = [{"name": "a", "sql": "A"}, {"name": "b", "sql": "B"}]
    rows = []
    window = timed_queries(Cursor(), queries, 3, rows, lambda: None)
    assert calls == ["A", "B"] * 3
    assert [r["warmup"] for r in rows] == [True, True, False, False, False, False]
    assert window["warmup_seconds"] == 2
    assert window["actual_seconds"] == 4
    assert window["completed_measured_queries"] == 4
    assert window["queries_per_minute"] == 60


def test_timed_queries_reject_changed_results(monkeypatch):
    values = iter([[(100,)], [(101,)]])
    cursor = SimpleNamespace(
        description=[("count",)],
        query_id="query",
        execute=lambda statement: None,
        fetchall=lambda: next(values),
    )
    with pytest.raises(ValueError, match="result changed"):
        timed_queries(cursor, [{"name": "a", "sql": "A"}], 1, [], lambda: None)


def test_wall_clock_stall_invalidates_trial(monkeypatch):
    clock = [0]
    timestamps = iter(["2026-11-03T00:00:00+00:00"] * 3 + ["2026-11-03T00:02:00+00:00"])

    def execute(statement):
        clock[0] += 1

    cursor = SimpleNamespace(
        description=[("count",)],
        query_id="query",
        execute=execute,
        fetchall=lambda: [(100,)],
    )
    monkeypatch.setattr(
        "nyc_taxi_lakehouse.warehouse_experiment.time.perf_counter", lambda: clock[0]
    )
    monkeypatch.setattr("nyc_taxi_lakehouse.warehouse_experiment.now", lambda: next(timestamps))
    with pytest.raises(ValueError, match="wall-clock bound"):
        timed_queries(cursor, [{"name": "a", "sql": "A"}], 1, [], lambda: None)


def test_billing_uses_signed_records_and_full_sample_cost_denominator():
    records = [
        {"record_id": str(i), "usage_quantity": dbu, "list_price_cost_usd": cost}
        for i, (dbu, cost) in enumerate([("2", "1.40"), ("-2", "-1.40"), ("1", "0.70")])
    ]
    totals = billing_totals(records, 100)
    assert totals["dbu_quantity"] == Decimal("1")
    assert totals["list_price_compute_estimate_usd"] == Decimal("0.70")
    assert totals["amortized_cost_per_measured_query_usd"] == Decimal("0.007")
    with pytest.raises(ValueError, match="Duplicate billing"):
        billing_totals([records[0], records[0]], 100)


def test_missing_or_unpriced_billing_is_not_zero_cost():
    assert billing_totals([], 100)["list_price_compute_estimate_usd"] is None
    records = [{"record_id": "id", "usage_quantity": "1", "list_price_cost_usd": None}]
    totals = billing_totals(records, 100)
    assert totals["status"] == "UNPRICED"
    assert totals["amortized_cost_per_measured_query_usd"] is None
    with pytest.raises(ValueError, match="denominator"):
        billing_totals(records, 0)


def test_connection_failure_stops_only_the_fresh_sample(monkeypatch, tmp_path):
    sql = pytest.importorskip("databricks.sql")
    stopped = []
    config = {"cluster_size": "2X-Small"}
    warehouse = SimpleNamespace(
        id="fresh-id",
        name=warehouse_name("sql_clean_test", "2xs_1"),
        cluster_size="2X-Small",
        as_dict=lambda: config,
        odbc_params=SimpleNamespace(path="/sql/fresh-id"),
    )
    client = SimpleNamespace(
        config=SimpleNamespace(host="https://example.test"),
        warehouses=SimpleNamespace(
            list=lambda: [],
            create=lambda **kwargs: SimpleNamespace(
                response=SimpleNamespace(id="fresh-id"),
                result=lambda **kwargs: warehouse,
            ),
            get=lambda warehouse_id: warehouse,
        ),
    )
    report = {
        "experiment_id": "sql_clean_test",
        "reference_warehouse": config,
        "samples": [],
    }

    def failed_connection(**kwargs):
        assert kwargs["_socket_timeout"] == 30
        assert kwargs["_retry_stop_after_attempts_count"] == 1
        raise RuntimeError("Connection failed.")

    monkeypatch.setattr(sql, "connect", failed_connection)
    monkeypatch.setattr("nyc_taxi_lakehouse.warehouse_experiment.WAREHOUSE_FIELDS", tuple(config))
    monkeypatch.setattr("nyc_taxi_lakehouse.warehouse_experiment.save_report", lambda *a, **k: None)
    monkeypatch.setattr(
        "nyc_taxi_lakehouse.warehouse_experiment.stop_warehouse",
        lambda client, warehouse_id: stopped.append(warehouse_id),
    )
    with pytest.raises(RuntimeError, match="Connection failed"):
        measure_sample(client, report, sample_plan()[0], tmp_path / "report.json", None)
    assert stopped == ["fresh-id"]
    assert report["samples"][0]["stopped_at"]
