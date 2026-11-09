from copy import deepcopy
from decimal import Decimal

import pytest

from nyc_taxi_lakehouse.compute_billing import collect_nodes, run_billing, validate_source
from nyc_taxi_lakehouse.compute_experiment import sample_plan


def test_missing_billing_is_pending_and_signed_corrections_are_preserved():
    assert run_billing([])["status"] == "PENDING"
    assert run_billing([])["list_price_compute_estimate_usd"] is None
    rows = [
        {"record_id": str(i), "usage_quantity": amount, "list_price_cost_usd": amount / 2}
        for i, amount in enumerate((Decimal("2"), Decimal("-2"), Decimal("1.5")))
    ]
    result = run_billing(rows)
    assert result["dbu_quantity"] == Decimal("1.5")
    assert result["list_price_compute_estimate_usd"] == Decimal("0.75")
    assert "amortized_cost_per_measured_query_usd" not in result
    with pytest.raises(ValueError, match="Duplicate billing record"):
        run_billing(rows + [rows[0]])


def test_billing_refuses_partial_samples_and_unowned_runs():
    plan = [
        {"phase": "prepare", "compute_type": "SERVERLESS", "sample_id": "unset"},
        *[{"phase": "measure", **row} for row in sample_plan()],
        {"phase": "audit", "compute_type": "SERVERLESS", "sample_id": "unset"},
    ]
    source = {
        "status": "MEASURED_AND_VERIFIED",
        "audit": {"status": "PASSED"},
        "samples": sample_plan(),
        "jobs": {"SERVERLESS": 1, "CLASSIC": 2},
        "executions": [
            {
                "run_id": i,
                "job_id": 1 if row["compute_type"] == "SERVERLESS" else 2,
                **row,
                "run": {"state": {"result_state": "SUCCESS"}},
            }
            for i, row in enumerate(plan)
        ],
    }
    validate_source(source)
    for change in (
        {"samples": source["samples"][:-1]},
        {"status": "RUNNING"},
        {"jobs": {"SERVERLESS": 3, "CLASSIC": 2}},
        {"executions": source["executions"][:-1]},
    ):
        invalid = {**deepcopy(source), **change}
        with pytest.raises(ValueError):
            validate_source(invalid)


def test_missing_node_inventory_is_explicit_and_unexpected_sql_errors_propagate(
    monkeypatch, caplog
):
    exc = pytest.importorskip("databricks.sql.exc")
    source = {
        "started_at": "2026-11-06T00:00:00+00:00",
        "samples": [
            {"compute_type": "CLASSIC", "classic_cluster": {"cluster_id": str(i)}} for i in range(3)
        ],
    }

    def denied(*args, **kwargs):
        raise exc.ServerOperationError("[INSUFFICIENT_PERMISSIONS] Cannot read node_timeline.")

    monkeypatch.setattr("nyc_taxi_lakehouse.compute_billing.query_rows", denied)
    assert collect_nodes(None, source, 1)["status"] == "UNAVAILABLE"
    assert "node identity evidence unavailable" in caplog.text

    def invalid(*args, **kwargs):
        raise exc.ServerOperationError("[PARSE_SYNTAX_ERROR] Invalid query.")

    monkeypatch.setattr("nyc_taxi_lakehouse.compute_billing.query_rows", invalid)
    with pytest.raises(exc.ServerOperationError, match="PARSE_SYNTAX_ERROR"):
        collect_nodes(None, source, 1)
