from copy import deepcopy
from types import SimpleNamespace

import pytest

from nyc_taxi_lakehouse.benchmark import (
    billing_report,
    collect_profiles,
    query_suite,
    result_digest,
    stop_warehouse,
    summarize,
    verify_results,
)
from nyc_taxi_lakehouse.benchmark_snapshot import (
    SnapshotConfig,
    assert_frozen_versions,
    bounded_months,
)


@pytest.mark.parametrize(
    "values",
    [
        {"end_month": "latest"},
        {"start_month": "2024-04", "end_month": "2024-03"},
        {"dataset_id": "unsafe; DROP TABLE trips"},
        {"catalog": "unsafe.catalog"},
    ],
)
def test_snapshot_rejects_unbounded_or_unsafe_configuration(values):
    with pytest.raises(ValueError):
        SnapshotConfig(**values)


def test_snapshot_selects_only_fixed_source_months(spark):
    frame = spark.createDataFrame(
        [(month,) for month in ("2023-12", "2024-01", "2024-03", "2024-04")],
        "source_month string",
    )
    assert sorted(
        row.source_month for row in bounded_months(frame, SnapshotConfig()).collect()
    ) == [
        "2024-01",
        "2024-03",
    ]


def test_frozen_version_audit_rejects_changed_or_missing_tables():
    expected = {"catalog.benchmark.trips": 0}
    assert_frozen_versions(expected, expected.copy())
    for observed in ({}, {"catalog.benchmark.trips": 1}):
        with pytest.raises(ValueError, match="Frozen table versions changed"):
            assert_frozen_versions(expected, observed)


def test_queries_render_only_snapshot_tables_and_have_stable_template_hashes():
    queries = query_suite(SnapshotConfig())
    other = query_suite(SnapshotConfig(dataset_id="q1_2024_comparison"))
    assert len(queries) == 4
    for baseline, comparison in zip(queries, other, strict=True):
        assert "{trips}" not in baseline["sql"]
        assert "nyc_taxi_dev.benchmark.q1_2024_" in baseline["sql"]
        assert baseline["sql"] != comparison["sql"]
        assert baseline["template_sha256"] == comparison["template_sha256"]


def measurements():
    return [
        {
            "query": "fares",
            "warmup": warmup,
            "client_elapsed_ms": elapsed,
            "result_rows": 1,
            "result_sha256": result_digest(["trips"], [(100,)]),
            "statement_id": f"statement-{index}",
        }
        for index, (warmup, elapsed) in enumerate(
            [(True, 9000), (False, 100), (False, 300), (False, 200)]
        )
    ]


def test_summary_excludes_warmup_and_keeps_missing_profiles_null():
    samples = measurements()
    samples[1]["profile"] = {
        "metrics": {"execution_time_ms": 50, "read_bytes": 0, "read_files_count": 0}
    }
    summary = summarize(samples)[0]
    assert summary["median_client_elapsed_ms"] == 200
    assert summary["min_client_elapsed_ms"] == 100
    assert summary["max_client_elapsed_ms"] == 300
    assert summary["samples"] == 3
    assert summary["profile_samples"] == 1
    assert summary["median_engine_execution_ms"] == 50
    assert summary["median_read_bytes"] == 0
    assert summary["median_spill_bytes"] is None


def test_changed_results_are_not_accepted_as_comparable_repetitions():
    samples = deepcopy(measurements())
    samples[-1]["result_sha256"] = result_digest(["trips"], [(101,)])
    with pytest.raises(ValueError, match="results changed"):
        verify_results(samples)


def test_missing_query_profiles_remain_explicitly_pending():
    pytest.importorskip("databricks.sdk")
    client = SimpleNamespace(
        query_history=SimpleNamespace(
            list=lambda **kwargs: SimpleNamespace(res=[], next_page_token=None)
        )
    )
    samples = measurements()
    assert collect_profiles(client, samples, timeout_seconds=0) == [
        "statement-0",
        "statement-1",
        "statement-2",
        "statement-3",
    ]
    assert all("profile" not in sample for sample in samples)


def test_query_profile_rejects_result_cache_hits():
    pytest.importorskip("databricks.sdk")
    query = SimpleNamespace(
        query_id="statement-0",
        is_final=True,
        metrics=object(),
        as_dict=lambda: {"metrics": {"result_from_cache": True}},
    )
    client = SimpleNamespace(
        query_history=SimpleNamespace(
            list=lambda **kwargs: SimpleNamespace(res=[query], next_page_token=None)
        )
    )
    with pytest.raises(ValueError, match="Result cache was used"):
        collect_profiles(client, measurements(), timeout_seconds=0)


def test_billing_access_denial_is_explicit_not_zero_cost(monkeypatch, caplog):
    exc = pytest.importorskip("databricks.sql.exc")

    def denied(*args, **kwargs):
        raise exc.ServerOperationError("[INSUFFICIENT_PERMISSIONS] Cannot read system.billing.")

    monkeypatch.setattr("nyc_taxi_lakehouse.benchmark.collect_billing", denied)
    report = billing_report(None, "nyc_taxi_dev", "job", "warehouse", {})
    assert report["status"] == "UNAVAILABLE"
    assert "Cannot read system.billing" in report["error"]
    assert "account administrator" in caplog.text
    assert "list_price_compute_estimate_usd" not in report


def test_billing_sql_errors_other_than_permissions_are_not_masked(monkeypatch):
    exc = pytest.importorskip("databricks.sql.exc")

    def invalid_sql(*args, **kwargs):
        raise exc.ServerOperationError("[PARSE_SYNTAX_ERROR] Invalid billing query.")

    monkeypatch.setattr("nyc_taxi_lakehouse.benchmark.collect_billing", invalid_sql)
    with pytest.raises(exc.ServerOperationError, match="PARSE_SYNTAX_ERROR"):
        billing_report(None, "nyc_taxi_dev", "job", "warehouse", {})


@pytest.mark.parametrize("states", [("RUNNING", "STOPPED"), ("RUNNING",) * 3])
def test_shutdown_verifies_compute_stays_stopped(monkeypatch, states):
    observed = iter(states)
    requests = []
    client = SimpleNamespace(
        warehouses=SimpleNamespace(
            stop=lambda warehouse_id: (
                requests.append(warehouse_id) or SimpleNamespace(result=lambda **kwargs: None)
            ),
            get=lambda warehouse_id: SimpleNamespace(state=SimpleNamespace(value=next(observed))),
        )
    )
    monkeypatch.setattr("nyc_taxi_lakehouse.benchmark.time.sleep", lambda seconds: None)
    if "STOPPED" in states:
        stop_warehouse(client, "owned-warehouse")
    else:
        with pytest.raises(RuntimeError, match="did not remain stopped"):
            stop_warehouse(client, "owned-warehouse")
    assert requests == ["owned-warehouse"] * len(states)


def test_shutdown_retries_timeout_and_checks_actual_state(monkeypatch, caplog):
    requests = []
    states = iter(("STOPPING", "STOPPED"))

    def stop(warehouse_id):
        requests.append(warehouse_id)

        def wait(**kwargs):
            raise TimeoutError("Warehouse is still stopping.")

        return SimpleNamespace(result=wait)

    client = SimpleNamespace(
        warehouses=SimpleNamespace(
            stop=stop,
            get=lambda warehouse_id: SimpleNamespace(state=SimpleNamespace(value=next(states))),
        )
    )
    monkeypatch.setattr("nyc_taxi_lakehouse.benchmark.time.sleep", lambda seconds: None)
    stop_warehouse(client, "owned-warehouse")
    assert requests == ["owned-warehouse", "owned-warehouse"]
    assert "stop request timed out" in caplog.text
