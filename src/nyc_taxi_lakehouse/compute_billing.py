import argparse
import hashlib
import io
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from nyc_taxi_lakehouse.benchmark import now, query_rows, stop_warehouse
from nyc_taxi_lakehouse.compute_experiment import ComputeConfig, sample_plan
from nyc_taxi_lakehouse.layout_experiment import connection, save
from nyc_taxi_lakehouse.warehouse_experiment import COLLECTOR_ID, billing_totals

QUERY = """
SELECT u.record_id, u.record_type, u.usage_start_time, u.usage_end_time,
       u.sku_name, u.usage_unit, u.usage_quantity, u.billing_origin_product,
       u.usage_metadata.cluster_id AS cluster_id,
       u.usage_metadata.job_id AS job_id, u.usage_metadata.job_run_id AS job_run_id,
       p.pricing.effective_list.default AS list_price_usd_per_dbu,
       u.usage_quantity*p.pricing.effective_list.default AS list_price_cost_usd
FROM system.billing.usage u LEFT JOIN system.billing.list_prices p
  ON u.sku_name=p.sku_name AND u.cloud=p.cloud AND u.usage_unit=p.usage_unit
 AND p.currency_code='USD' AND u.usage_start_time>=p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time<p.price_end_time)
WHERE u.usage_metadata.job_id=:job_id AND u.usage_metadata.job_run_id=:run_id
  AND u.billing_origin_product='JOBS' AND u.cloud='AZURE' AND u.usage_unit='DBU'
  AND u.usage_date BETWEEN :first_date AND :last_date
ORDER BY u.usage_start_time, u.record_id
"""

NODE_QUERY = """
SELECT cluster_id, instance_id, driver, node_type,
       min(start_time) AS first_observed_at, max(end_time) AS last_observed_at,
       count(*) AS minute_records
FROM system.compute.node_timeline
WHERE workspace_id=:workspace_id AND cluster_id IN (:cluster_1, :cluster_2, :cluster_3)
  AND start_time >= :started_at AND start_time <= :collected_at
GROUP BY cluster_id, instance_id, driver, node_type
ORDER BY cluster_id, driver, instance_id
"""


def collect_nodes(cursor, source, workspace_id):
    from databricks.sql.exc import ServerOperationError

    clusters = [
        row["classic_cluster"]["cluster_id"]
        for row in source["samples"]
        if row["compute_type"] == "CLASSIC"
    ]
    try:
        rows = query_rows(
            cursor,
            NODE_QUERY,
            {
                **{f"cluster_{i}": value for i, value in enumerate(clusters, 1)},
                "workspace_id": str(workspace_id),
                "started_at": datetime.fromisoformat(source["started_at"]),
                "collected_at": datetime.now(UTC),
            },
        )
    except ServerOperationError as error:
        if not str(error).startswith(
            ("[INSUFFICIENT_PERMISSIONS]", "[TABLE_OR_VIEW_NOT_FOUND]", "[SCHEMA_NOT_FOUND]")
        ):
            raise
        logging.warning("Classic node identity evidence unavailable: %s", error)
        return {"status": "UNAVAILABLE", "error": str(error)}
    return {
        "status": "OBSERVED_RECORDS" if rows else "PENDING",
        "records": rows,
        "collector_statement_id": cursor.query_id,
        "scope": (
            "Node identity mapping only. Minute-granularity observations are not complete "
            "Azure allocated lifetimes and must not be used as VM/disk billing durations."
        ),
    }


def validate_source(source):
    if source["status"] != "MEASURED_AND_VERIFIED" or source["audit"]["status"] != "PASSED":
        raise ValueError("Billing requires a complete, verified compute comparison.")
    if [
        {key: row[key] for key in ("sample_id", "compute_type")} for row in source["samples"]
    ] != sample_plan():
        raise ValueError("Billing requires all six planned compute samples.")
    expected = [
        {"phase": "prepare", "compute_type": "SERVERLESS", "sample_id": "unset"},
        *[{"phase": "measure", **row} for row in sample_plan()],
        {"phase": "audit", "compute_type": "SERVERLESS", "sample_id": "unset"},
    ]
    if [
        {key: row[key] for key in ("phase", "compute_type", "sample_id")}
        for row in source["executions"]
    ] != expected or len({row["run_id"] for row in source["executions"]}) != 8:
        raise ValueError("Expected distinct prepare, six measurement, and audit runs.")
    for row in source["executions"]:
        if (
            row["job_id"] != source["jobs"][row["compute_type"]]
            or row["run"]["state"]["result_state"] != "SUCCESS"
        ):
            raise ValueError("Unexpected job ownership or unsuccessful execution.")


def run_billing(records):
    result = billing_totals(records, 1)
    result.pop("amortized_cost_per_measured_query_usd")
    return {
        **result,
        "records": records,
        "scope": "Databricks DBUs only; classic Azure VM/disk charges are not included.",
    }


def refresh(client, source_path, collection_id):
    if not re.fullmatch(r"compute_billing_[a-z0-9_]{1,48}", collection_id):
        raise ValueError("Use a fresh compute_billing_ collection ID.")
    payload = source_path.read_bytes()
    source = json.loads(payload)
    validate_source(source)
    config = ComputeConfig(experiment_id=source["experiment_id"], end_month="2024-03")
    original = client.files.download(str(config.root / "experiment_report.json")).contents.read()
    if original != payload:
        raise ValueError("Local and archived measurement reports differ.")
    path = Path("benchmark-results") / f"{collection_id}.json"
    if path.exists():
        raise ValueError("Billing report exists; choose a fresh collection ID.")
    for row in source["samples"]:
        if row["compute_type"] == "CLASSIC":
            cluster = client.clusters.get(row["classic_cluster"]["cluster_id"])
            if cluster.state.value != "TERMINATED":
                raise ValueError("Classic sample cluster must be terminated before billing.")
    collector = client.warehouses.get(COLLECTOR_ID)
    if (
        collector.name != "nyc-taxi-dev-benchmark"
        or collector.state.value != "STOPPED"
        or collector.num_active_sessions
    ):
        raise ValueError("Use only the stopped, unused, dedicated benchmark collector.")
    root = f"/Volumes/{config.catalog}/benchmark/reports/billing_refreshes/{collection_id}"
    report = {
        "collection_id": collection_id,
        "experiment_id": config.experiment_id,
        "status": "RUNNING",
        "started_at": now(),
        "source_report_sha256": hashlib.sha256(payload).hexdigest(),
        "source_report": str(config.root / "experiment_report.json"),
        "remote_report": f"{root}/report.json",
        "collector_warehouse_id": COLLECTOR_ID,
        "collector_sources": {},
        "runs": [],
        "total_cost_status": "PENDING_AZURE_INFRASTRUCTURE_AND_FINAL_BILLING",
        "scope": (
            "Read-only billing refresh by exact job/run ID, including signed corrections, "
            "priced at effective USD list prices. Prepare/audit are separate from measurements. "
            "Records remain provisional; missing records are not zero cost. Classic DBUs "
            "exclude separately billed Azure VM/disk costs, so this is not a total-cost "
            "comparison. Collector, storage, discounts, credits and taxes are excluded."
        ),
    }
    for name in (
        Path(__file__).name,
        "warehouse_experiment.py",
        "layout_experiment.py",
        "benchmark.py",
        "compute_experiment.py",
    ):
        module = Path(__file__).with_name(name)
        content = module.read_bytes()
        remote = f"{root}/collector_sources/{module.name}"
        client.files.upload(remote, io.BytesIO(content), overwrite=False)
        report["collector_sources"][module.name] = {
            "sha256": hashlib.sha256(content).hexdigest(),
            "remote_path": remote,
        }
    save(client, path, report)
    try:
        client.warehouses.start(COLLECTOR_ID).result(timeout=timedelta(minutes=10))
        with connection(client, client.warehouses.get(COLLECTOR_ID)) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC'")
                all_ids = set()
                for execution in source["executions"]:
                    records = query_rows(
                        cursor,
                        QUERY,
                        {
                            "job_id": str(execution["job_id"]),
                            "run_id": str(execution["run_id"]),
                            "first_date": datetime.fromisoformat(source["started_at"]).date(),
                            "last_date": datetime.now(UTC).date(),
                        },
                    )
                    billing = run_billing(records)
                    ids = {row["record_id"] for row in records}
                    if all_ids & ids:
                        raise ValueError("Billing records overlap distinct job runs.")
                    all_ids.update(ids)
                    report["runs"].append(
                        {
                            **{
                                key: execution[key]
                                for key in (
                                    "phase",
                                    "compute_type",
                                    "sample_id",
                                    "job_id",
                                    "run_id",
                                )
                            },
                            "billing": billing,
                            "collected_at": now(),
                            "collector_statement_id": cursor.query_id,
                        }
                    )
                    save(client, path, report)
                report["classic_node_inventory"] = collect_nodes(
                    cursor, source, client.get_workspace_id()
                )
        if source_path.read_bytes() != payload:
            raise ValueError("Measurement report changed during billing collection.")
        report["status"] = "COLLECTED"
        report["completed_at"] = now()
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        logging.exception("Compute billing refresh failed; partial evidence retained.")
        raise
    finally:
        stop_warehouse(client, COLLECTOR_ID)
        report["collector_stopped_at"] = now()
        save(client, path, report)
    return report


def main():
    parser = argparse.ArgumentParser(description="Refresh compute DBUs without rerunning samples.")
    parser.add_argument("--experiment-report", type=Path, required=True)
    parser.add_argument("--collection-id", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("databricks.sql").setLevel(logging.WARNING)
    from databricks.sdk import WorkspaceClient

    refresh(WorkspaceClient(auth_type="azure-cli"), args.experiment_report, args.collection_id)


if __name__ == "__main__":
    main()
