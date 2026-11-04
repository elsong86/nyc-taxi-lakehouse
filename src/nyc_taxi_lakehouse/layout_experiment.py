import argparse
import hashlib
import io
import json
import logging
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

from nyc_taxi_lakehouse.benchmark import (
    collect_profiles,
    delta_write_metrics,
    frozen_versions,
    now,
    query_rows,
    query_suite,
    result_digest,
    software_versions,
    stop_warehouse,
    summarize,
    verify_results,
)
from nyc_taxi_lakehouse.benchmark_snapshot import SnapshotConfig, assert_frozen_versions
from nyc_taxi_lakehouse.warehouse_experiment import COLLECTOR_ID, billing_totals

LOGGER = logging.getLogger(__name__)
BASE = SnapshotConfig()
TARGET_BYTES = 64 * 1024 * 1024
KEYS = ["source_month", "pickup_at"]


def target_table(experiment_id, layout):
    if not re.fullmatch(r"layout_[a-z0-9_]{1,32}", experiment_id):
        raise ValueError("Use a short, isolated layout_ experiment ID.")
    if layout not in ("partitioned", "liquid"):
        raise ValueError("Unknown table layout.")
    return f"{BASE.catalog}.benchmark.{experiment_id}_{layout}_trips"


def creation_sql(experiment_id, layout, source_version):
    table = target_table(experiment_id, layout)
    if not isinstance(source_version, int) or source_version < 0:
        raise ValueError("Source version must be a nonnegative integer.")
    clause = (
        "PARTITIONED BY (source_month)"
        if layout == "partitioned"
        else "CLUSTER BY (" + ", ".join(KEYS) + ")"
    )
    return (
        f"CREATE TABLE {table} USING DELTA {clause} "
        f"TBLPROPERTIES ('delta.targetFileSize'='{TARGET_BYTES}', "
        "'delta.parquet.compression.codec'='zstd') "
        f"AS SELECT * FROM {BASE.table('trips')} VERSION AS OF {source_version}"
    )


def layout_queries(experiment_id, layout):
    table = target_table(experiment_id, layout)
    return [{**q, "sql": q["sql"].replace(BASE.table("trips"), table)} for q in query_suite(BASE)]


def detail(cursor, table):
    row = query_rows(cursor, f"DESCRIBE DETAIL {table}")[0]
    for key in ("partitionColumns", "clusteringColumns", "properties"):
        if isinstance(row[key], str):
            row[key] = json.loads(row[key])
    return {
        "table": table,
        "active_data_bytes": row["sizeInBytes"],
        "num_files": row["numFiles"],
        "partition_columns": row["partitionColumns"],
        "clustering_columns": row["clusteringColumns"],
        "properties": row["properties"],
    }


def optimize_effects(results):
    metrics = [
        json.loads(row["metrics"]) if isinstance(row["metrics"], str) else row["metrics"]
        for row in results
    ]
    if not metrics:
        raise ValueError("OPTIMIZE returned no operation metrics.")
    tasks = [
        row["clusteringMetrics"]["numClusteringTasksPlanned"]
        for row in metrics
        if row.get("clusteringMetrics") is not None
    ]
    added = sum(row["numFilesAdded"] for row in metrics)
    removed = sum(row["numFilesRemoved"] for row in metrics)
    return {
        "data_files_added": added,
        "data_files_removed": removed,
        "data_bytes_added": sum(row["filesAdded"]["totalSize"] for row in metrics),
        "data_bytes_removed": sum(row["filesRemoved"]["totalSize"] for row in metrics),
        "physical_rewrite_observed": bool(added or removed),
        "clustering_tasks_planned": sum(tasks) if tasks else None,
    }


def save(client, path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, indent=2, default=str).encode()
    path.write_bytes(payload)
    client.files.upload(report["remote_report"], io.BytesIO(payload), overwrite=True)


def connection(client, warehouse):
    from databricks import sql

    return sql.connect(
        server_hostname=urlparse(client.config.host).hostname,
        http_path=warehouse.odbc_params.path,
        credentials_provider=lambda: client.config.authenticate,
        user_agent_entry="nyc-taxi-layout-experiment",
        _socket_timeout=30,
        _retry_stop_after_attempts_count=1,
        _retry_stop_after_attempts_duration=60,
    )


def check_versions(cursor, report):
    assert_frozen_versions(
        report["snapshot_versions"],
        frozen_versions(cursor, {"snapshot_versions": report["snapshot_versions"]}),
    )
    if report.get("target_versions"):
        assert_frozen_versions(
            report["target_versions"],
            frozen_versions(cursor, {"snapshot_versions": report["target_versions"]}),
        )


def prepare_layout(client, report, layout, reference, path):
    from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType

    table = target_table(report["experiment_id"], layout)
    name = f"nyc-taxi-dev-{report['experiment_id']}-{layout}-benchmark"
    if any(w.name == name for w in client.warehouses.list()):
        raise ValueError("Preparation warehouse already exists; refusing destructive reuse.")
    item = {"layout": layout, "table": table, "warehouse_name": name, "started_at": now()}
    report["layouts"].append(item)
    save(client, path, report)
    try:
        waiter = client.warehouses.create(
            name=name,
            cluster_size="2X-Small",
            warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
            enable_photon=True,
            enable_serverless_compute=True,
            min_num_clusters=1,
            max_num_clusters=1,
            auto_stop_mins=5,
            channel=reference.channel,
        )
        item["warehouse_id"] = waiter.response.id
        save(client, path, report)
        warehouse = waiter.result(timeout=timedelta(minutes=10))
        with connection(client, warehouse) as conn, conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            check_versions(cursor, report)
            started = time.perf_counter()
            cursor.execute(
                creation_sql(
                    report["experiment_id"],
                    layout,
                    report["snapshot_versions"][BASE.table("trips")],
                )
            )
            cursor.fetchall()
            item["materialize_seconds"] = time.perf_counter() - started
            item["materialize_statement_id"] = cursor.query_id
            item["before_optimize"] = detail(cursor, table)
            started = time.perf_counter()
            cursor.execute(f"OPTIMIZE {table}" + (" FULL" if layout == "liquid" else ""))
            item["optimize_result"] = [
                dict(zip([c[0] for c in cursor.description], row, strict=True))
                for row in cursor.fetchall()
            ]
            item["optimize_seconds"] = time.perf_counter() - started
            item["optimize_statement_id"] = cursor.query_id
            item["optimize_effects"] = optimize_effects(item["optimize_result"])
            item["after_optimize"] = detail(cursor, table)
            item["history"] = query_rows(cursor, f"DESCRIBE HISTORY {table}")
            item["version"] = max(row["version"] for row in item["history"])
            item["write_metrics"] = delta_write_metrics(item["history"], -1, item["version"])
            expected = (["source_month"], []) if layout == "partitioned" else ([], KEYS)
            observed = (
                item["after_optimize"]["partition_columns"],
                item["after_optimize"]["clustering_columns"],
            )
            if observed != expected:
                raise ValueError("Observed physical layout does not match requested layout.")
            check_versions(cursor, report)
    finally:
        if item.get("warehouse_id"):
            warehouse = client.warehouses.get(item["warehouse_id"])
            if warehouse.name != name:
                raise ValueError("Preparation warehouse ownership mismatch.")
            stop_warehouse(client, item["warehouse_id"])
            item["stopped_at"] = now()
            save(client, path, report)
        else:
            LOGGER.error("Creation failed without a warehouse ID; inspect %s.", name)


def audit_and_measure(client, report, reference, path):
    reference = client.warehouses.get(reference.id)
    if reference.state.value != "STOPPED" or reference.num_active_sessions:
        raise ValueError("Query/audit warehouse must be stopped and unused.")
    try:
        client.warehouses.start(reference.id).result(timeout=timedelta(minutes=10))
        with connection(client, reference) as conn, conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            check_versions(cursor, report)
            report["engine_version"] = query_rows(cursor, "SELECT current_version() AS version")
            for item in report["layouts"]:
                table = item["table"]
                differences = query_rows(
                    cursor,
                    "SELECT count(*) AS differences FROM ("
                    f"(SELECT * FROM {BASE.table('trips')} EXCEPT ALL SELECT * FROM {table}) "
                    "UNION ALL "
                    f"(SELECT * FROM {table} EXCEPT ALL SELECT * FROM {BASE.table('trips')})"
                    ")",
                )[0]["differences"]
                count = query_rows(cursor, f"SELECT count(*) AS rows FROM {table}")[0]["rows"]
                if differences or count != report["expected_rows"]:
                    raise ValueError("Layout copy failed exact row-multiset audit.")
                item["audit"] = {"status": "PASSED", "rows": count, "differences": differences}
                save(client, path, report)
            cursor.execute("SET use_cached_result = false")
            setting = query_rows(cursor, "SET use_cached_result")
            if not setting or str(next(reversed(setting[0].values()))).lower() != "false":
                raise ValueError("Result caching is not disabled.")
            order = (
                ("partitioned", 1),
                ("liquid", 1),
                ("liquid", 2),
                ("partitioned", 2),
                ("partitioned", 3),
                ("liquid", 3),
            )
            for layout, repetition in order:
                for query in layout_queries(report["experiment_id"], layout):
                    for iteration in range(4):
                        started_at = now()
                        started = time.perf_counter()
                        cursor.execute(query["sql"])
                        rows = cursor.fetchall()
                        elapsed = (time.perf_counter() - started) * 1000
                        report["measurements"].append(
                            {
                                "query": query["name"],
                                "layout": layout,
                                "sample_id": f"{layout}_{repetition}",
                                "iteration": iteration,
                                "warmup": iteration == 0,
                                "statement_id": cursor.query_id,
                                "started_at": started_at,
                                "client_elapsed_ms": elapsed,
                                "result_rows": len(rows),
                                "result_sha256": result_digest(
                                    [c[0] for c in cursor.description], rows
                                ),
                            }
                        )
                save(client, path, report)
            verify_results(report["measurements"])
            check_versions(cursor, report)
    finally:
        stop_warehouse(client, reference.id)
        report["query_warehouse_stopped_at"] = now()
        save(client, path, report)
    pending = []
    for index in range(0, len(report["measurements"]), 50):
        pending.extend(collect_profiles(client, report["measurements"][index : index + 50]))
    report["pending_statement_ids"] = pending
    report["summary"] = [
        {
            "layout": layout,
            "queries": summarize([m for m in report["measurements"] if m["layout"] == layout]),
        }
        for layout in ("partitioned", "liquid")
    ]
    if pending:
        raise ValueError("Query profiles incomplete; recorded measurements must not be rerun.")
    report["status"] = "MEASURED_AND_VERIFIED"
    save(client, path, report)


def run(client, experiment_id, path):
    target_table(experiment_id, "partitioned")
    if path.exists():
        raise ValueError("Report already exists; inspect/resume evidence, do not rerun layouts.")
    reference = client.warehouses.get(COLLECTOR_ID)
    if (
        reference.name != "nyc-taxi-dev-benchmark"
        or reference.cluster_size != "2X-Small"
        or reference.state.value != "STOPPED"
        or reference.num_active_sessions
        or reference.min_num_clusters != 1
        or reference.max_num_clusters != 1
        or not reference.enable_photon
        or not reference.enable_serverless_compute
    ):
        raise ValueError("Use only the stopped, unchanged baseline 2X-Small warehouse.")
    report = {
        "experiment_id": experiment_id,
        "status": "RUNNING",
        "remote_report": str(
            BASE.report_root / "layout_experiments" / experiment_id / "experiment_report.json"
        ),
        "software": {
            **software_versions(),
            "layout_experiment_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        },
        "reference_warehouse": reference.as_dict(),
        "layouts": [],
        "measurements": [],
        "scope": (
            "Fresh month-partitioned and liquid-clustered copies of the same immutable Q1 "
            "Silver, common ZSTD codec and 64 MiB best-effort OPTIMIZE target. Liquid keys "
            "source_month,pickup_at match weekly filters. Separate fresh 2X-Small warehouses "
            "for each CTAS/optimization phase; exact row audits and all query measurements "
            "use a different fixed 2X-Small warehouse. Partitioned OPTIMIZE compacts; liquid "
            "OPTIMIZE FULL requests one-time reclustering but can plan no physical writes. "
            "This is setup/reclustering, not a "
            "steady-state incremental maintenance benchmark. Three interleaved sessions per "
            "layout, one warmup and three measured repeats per query. Gold remains unchanged "
            "as a control. Warm I/O caches and local connector overhead uncontrolled. "
            "No production/frozen tables changed; no query-serving cost comparison claimed."
        ),
    }
    save(client, path, report)
    try:
        client.warehouses.start(reference.id).result(timeout=timedelta(minutes=10))
        try:
            with connection(client, reference) as conn, conn.cursor() as cursor:
                dataset = query_rows(
                    cursor,
                    f"SELECT status, metadata_json FROM {BASE.catalog}.benchmark.datasets "
                    "WHERE dataset_id=:dataset_id",
                    {"dataset_id": BASE.dataset_id},
                )
                if len(dataset) != 1 or dataset[0]["status"] != "READY":
                    raise ValueError("Frozen Q1 snapshot is not uniquely READY.")
                metadata = json.loads(dataset[0]["metadata_json"])
                report["snapshot_versions"] = metadata["snapshot_versions"]
                report["expected_rows"] = metadata["expected_counts"]["trips"]
                report["baseline_detail"] = detail(cursor, BASE.table("trips"))
                report["catalog_description"] = query_rows(
                    cursor, f"DESCRIBE CATALOG EXTENDED {BASE.catalog}"
                )
                predictive = [
                    row
                    for row in report["catalog_description"]
                    if any("predictive" in str(value).lower() for value in row.values())
                ]
                if not predictive or not any(
                    "disable" in str(value).lower() for row in predictive for value in row.values()
                ):
                    raise ValueError("Catalog predictive optimization must remain disabled.")
                check_versions(cursor, report)
                for layout in ("partitioned", "liquid"):
                    name = target_table(experiment_id, layout).rsplit(".", 1)[1]
                    existing = query_rows(
                        cursor,
                        f"SELECT count(*) AS tables FROM {BASE.catalog}.information_schema.tables "
                        "WHERE table_schema='benchmark' AND table_name=:name",
                        {"name": name},
                    )
                    if existing[0]["tables"]:
                        raise ValueError("Target table already exists; refusing overwrite.")
        finally:
            stop_warehouse(client, reference.id)
        for layout in ("partitioned", "liquid"):
            prepare_layout(client, report, layout, reference, path)
        report["target_versions"] = {item["table"]: item["version"] for item in report["layouts"]}
        audit_and_measure(client, report, reference, path)
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        LOGGER.exception("Layout experiment failed; partial evidence preserved.")
        raise
    finally:
        save(client, path, report)
    return report


def refresh_billing(client, report, path):
    if report["status"] != "MEASURED_AND_VERIFIED":
        raise ValueError("Billing requires a verified layout experiment.")
    reference = client.warehouses.get(COLLECTOR_ID)
    if reference.state.value != "STOPPED" or reference.num_active_sessions:
        raise ValueError("Billing collector must be stopped and unused.")
    for item in report["layouts"]:
        warehouse = client.warehouses.get(item["warehouse_id"])
        if warehouse.name != item["warehouse_name"] or warehouse.state.value != "STOPPED":
            raise ValueError("Preparation warehouse is not owned and stopped.")
    try:
        client.warehouses.start(reference.id).result(timeout=timedelta(minutes=10))
        with connection(client, reference) as conn, conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            for item in report["layouts"]:
                records = query_rows(
                    cursor,
                    """
                    SELECT u.record_id, u.record_type, u.usage_start_time, u.usage_end_time,
                           u.sku_name, u.usage_unit, u.usage_quantity,
                           p.pricing.effective_list.default AS list_price_usd_per_dbu,
                           u.usage_quantity*p.pricing.effective_list.default AS list_price_cost_usd
                    FROM system.billing.usage u LEFT JOIN system.billing.list_prices p
                      ON u.sku_name=p.sku_name AND u.cloud=p.cloud AND u.usage_unit=p.usage_unit
                     AND p.currency_code='USD' AND u.usage_start_time>=p.price_start_time
                     AND (p.price_end_time IS NULL OR u.usage_start_time<p.price_end_time)
                    WHERE u.usage_metadata.warehouse_id=:warehouse_id
                      AND u.cloud='AZURE' AND u.usage_unit='DBU'
                      AND u.usage_date BETWEEN :first_date AND :last_date
                    ORDER BY u.usage_start_time, u.record_id
                    """,
                    {
                        "warehouse_id": item["warehouse_id"],
                        "first_date": datetime.fromisoformat(item["started_at"]).date(),
                        "last_date": datetime.now(UTC).date(),
                    },
                )
                totals = billing_totals(records, 1)
                totals.pop("amortized_cost_per_measured_query_usd")
                item["billing"] = {**totals, "records": records, "collected_at": now()}
        report["billing_scope"] = (
            "Entire dedicated materialization/optimization warehouse usage, including signed "
            "corrections at effective USD list prices, startup and metadata. Exact audits, "
            "query comparisons and billing collection run elsewhere and are excluded. "
            "Not stage-level bills or repeated maintenance observations. Missing is pending, "
            "not zero; observed records are provisional. Discounts, credits, tax and "
            "storage excluded."
        )
        save(client, path, report)
    finally:
        stop_warehouse(client, reference.id)
        report["billing_collector_stopped_at"] = now()
        save(client, path, report)


def main():
    parser = argparse.ArgumentParser(
        description="Isolated partitioned vs liquid-clustered Q1 tables."
    )
    parser.add_argument("--phase", choices=("measure", "billing"), required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    target_table(args.experiment_id, "partitioned")
    from databricks.sdk import WorkspaceClient

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("databricks.sql").setLevel(logging.WARNING)
    client = WorkspaceClient(auth_type="azure-cli")
    path = args.report or Path("benchmark-results") / f"{args.experiment_id}.json"
    if args.phase == "measure":
        run(client, args.experiment_id, path)
    else:
        report = json.loads(path.read_text())
        if report["experiment_id"] != args.experiment_id:
            raise ValueError("Report belongs to a different layout experiment.")
        refresh_billing(client, report, path)


if __name__ == "__main__":
    main()
