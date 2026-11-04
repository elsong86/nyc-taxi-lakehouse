import argparse
import hashlib
import io
import json
import logging
import re
import statistics
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse

from nyc_taxi_lakehouse.benchmark import (
    collect_profiles,
    digest,
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

LOGGER = logging.getLogger(__name__)
COLLECTOR_ID = "919966835b70007d"
WAREHOUSE_FIELDS = (
    "cluster_size",
    "warehouse_type",
    "min_num_clusters",
    "max_num_clusters",
    "enable_photon",
    "enable_serverless_compute",
    "auto_stop_mins",
    "channel",
)


def sample_plan():
    return [
        {"sample_id": sample_id, "size": size}
        for sample_id, size in (
            ("2xs_1", "2X-Small"),
            ("xs_1", "X-Small"),
            ("xs_2", "X-Small"),
            ("2xs_2", "2X-Small"),
            ("2xs_3", "2X-Small"),
            ("xs_3", "X-Small"),
        )
    ]


def warehouse_name(experiment_id, sample_id):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", experiment_id):
        raise ValueError("Experiment ID must be a short lowercase SQL identifier.")
    if sample_id not in {row["sample_id"] for row in sample_plan()}:
        raise ValueError("Unknown sample ID.")
    return f"nyc-taxi-dev-clean-{experiment_id}-{sample_id}-benchmark"


def save_report(client, path, report, *, remote=True):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, indent=2, default=str).encode()
    path.write_bytes(payload)
    if remote:
        client.files.upload(report["remote_report"], io.BytesIO(payload), overwrite=True)


def timed_queries(cursor, queries, duration_seconds, measurements, checkpoint):
    if duration_seconds <= 0 or not queries:
        raise ValueError("A positive duration and nonempty query suite are required.")
    warmup_started = time.perf_counter()
    signatures = {}

    def execute(query, iteration, warmup):
        started_at = now()
        started = time.perf_counter()
        cursor.execute(query["sql"])
        rows = cursor.fetchall()
        elapsed = (time.perf_counter() - started) * 1000
        columns = [column[0] for column in cursor.description]
        signature = (result_digest(columns, rows), len(rows))
        if query["name"] in signatures and signatures[query["name"]] != signature:
            raise ValueError(f"Query result changed: {query['name']}")
        signatures[query["name"]] = signature
        measurements.append(
            {
                "query": query["name"],
                "iteration": iteration,
                "warmup": warmup,
                "statement_id": cursor.query_id,
                "started_at": started_at,
                "client_elapsed_ms": elapsed,
                "result_rows": len(rows),
                "result_sha256": signature[0],
            }
        )

    for query in queries:
        execute(query, 0, True)
    warmup_seconds = time.perf_counter() - warmup_started
    started_at = now()
    started = time.perf_counter()
    cycles = 0
    while time.perf_counter() - started < duration_seconds:
        cycles += 1
        for query in queries:
            execute(query, cycles, False)
        if cycles % 10 == 0:
            checkpoint()
    elapsed = time.perf_counter() - started
    ended_at = now()
    wall_seconds = (
        datetime.fromisoformat(ended_at) - datetime.fromisoformat(started_at)
    ).total_seconds()
    if wall_seconds > duration_seconds + 60:
        raise ValueError("Query window exceeded its wall-clock bound; trial is invalid.")
    return {
        "started_at": started_at,
        "ended_at": ended_at,
        "wall_seconds": wall_seconds,
        "warmup_seconds": warmup_seconds,
        "actual_seconds": elapsed,
        "completed_cycles": cycles,
        "completed_measured_queries": cycles * len(queries),
        "queries_per_minute": cycles * len(queries) * 60 / elapsed,
    }


def owned_warehouse(client, report, sample):
    warehouse = client.warehouses.get(sample["warehouse_id"])
    expected_name = warehouse_name(report["experiment_id"], sample["sample_id"])
    if warehouse.name != expected_name or warehouse.cluster_size != sample["size"]:
        raise ValueError("Warehouse does not match the recorded dedicated sample.")
    return warehouse


def finish_sample(client, report, sample, path):
    warehouse = owned_warehouse(client, report, sample)
    if warehouse.state.value != "STOPPED":
        started = time.perf_counter()
        stop_warehouse(client, warehouse.id)
        sample["shutdown_seconds"] = time.perf_counter() - started
        sample["stopped_at"] = now()
        sample["lifecycle_seconds"] = (
            datetime.fromisoformat(sample["stopped_at"])
            - datetime.fromisoformat(sample["lifecycle_started_at"])
        ).total_seconds()
        save_report(client, path, report)
    if not sample.get("stopped_at"):
        raise ValueError("Sample has no recorded verified shutdown boundary.")
    pending = []
    rows = sample["measurements"]
    for index in range(0, len(rows), 50):
        pending.extend(collect_profiles(client, rows[index : index + 50]))
    sample["pending_statement_ids"] = pending
    for row in rows:
        profile = row.get("profile", {})
        if profile.get("endpoint_id") not in (None, sample["warehouse_id"]):
            raise ValueError("Query profile belongs to a different warehouse.")
    sample["summary"] = summarize(rows)
    sample["status"] = "PROFILES_PENDING" if pending else "COMPLETE"
    save_report(client, path, report)


def measure_sample(client, report, plan, path, channel):
    from databricks import sql
    from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType

    config = SnapshotConfig()
    name = warehouse_name(report["experiment_id"], plan["sample_id"])
    if any(warehouse.name == name for warehouse in client.warehouses.list()):
        raise ValueError("Sample warehouse already exists; refusing to reuse or rerun it.")
    sample = {
        **plan,
        "status": "CREATING",
        "warehouse_name": name,
        "lifecycle_started_at": now(),
        "measurements": [],
    }
    report["samples"].append(sample)
    save_report(client, path, report)
    started = time.perf_counter()
    try:
        waiter = client.warehouses.create(
            name=name,
            cluster_size=plan["size"],
            min_num_clusters=1,
            max_num_clusters=1,
            enable_photon=True,
            enable_serverless_compute=True,
            auto_stop_mins=5,
            channel=channel,
            warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        )
        sample["warehouse_id"] = waiter.response.id
        save_report(client, path, report)
        warehouse = waiter.result(timeout=timedelta(minutes=10))
        sample["startup_seconds"] = time.perf_counter() - started
        sample["warehouse_config"] = {key: warehouse.as_dict()[key] for key in WAREHOUSE_FIELDS}
        expected = {
            **report["reference_warehouse"],
            "cluster_size": plan["size"],
        }
        if sample["warehouse_config"] != expected:
            raise ValueError("New warehouse configuration differs from the fixed controls.")
        sample["status"] = "RUNNING"
        save_report(client, path, report)
        with (
            sql.connect(
                server_hostname=urlparse(client.config.host).hostname,
                http_path=warehouse.odbc_params.path,
                credentials_provider=lambda: client.config.authenticate,
                user_agent_entry="nyc-taxi-isolated-sql-experiment",
                _socket_timeout=30,
                _retry_stop_after_attempts_count=1,
                _retry_stop_after_attempts_duration=60,
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute("SET use_cached_result = false")
            setting = query_rows(cursor, "SET use_cached_result")
            if not setting or str(next(reversed(setting[0].values()))).lower() != "false":
                raise ValueError("SQL result caching was not disabled.")
            datasets = query_rows(
                cursor,
                f"SELECT status, metadata_json FROM {config.catalog}.benchmark.datasets "
                "WHERE dataset_id = :dataset_id",
                {"dataset_id": config.dataset_id},
            )
            if len(datasets) != 1 or datasets[0]["status"] != "READY":
                raise ValueError("Frozen dataset is not uniquely registered and READY.")
            snapshot = json.loads(datasets[0]["metadata_json"])
            if snapshot["start_month"] != "2024-01" or snapshot["end_month"] != "2024-03":
                raise ValueError("Query suite requires January-March 2024.")
            assert_frozen_versions(snapshot["snapshot_versions"], frozen_versions(cursor, snapshot))
            if "snapshot_versions" in report:
                assert_frozen_versions(report["snapshot_versions"], snapshot["snapshot_versions"])
            report["snapshot_versions"] = snapshot["snapshot_versions"]
            sample["engine_version"] = query_rows(cursor, "SELECT current_version() AS version")
            sample["query_window"] = timed_queries(
                cursor,
                report["workload"],
                report["duration_seconds"],
                sample["measurements"],
                lambda: save_report(client, path, report, remote=False),
            )
            assert_frozen_versions(snapshot["snapshot_versions"], frozen_versions(cursor, snapshot))
        sample["status"] = "MEASURED"
        save_report(client, path, report)
    finally:
        if "warehouse_id" in sample:
            shutdown_started = time.perf_counter()
            owned_warehouse(client, report, sample)
            stop_warehouse(client, sample["warehouse_id"])
            sample["shutdown_seconds"] = time.perf_counter() - shutdown_started
            sample["stopped_at"] = now()
            sample["lifecycle_seconds"] = (
                datetime.fromisoformat(sample["stopped_at"])
                - datetime.fromisoformat(sample["lifecycle_started_at"])
            ).total_seconds()
            save_report(client, path, report)
        else:
            LOGGER.error("Creation failed before an ID was returned; inspect warehouse %s.", name)
    finish_sample(client, report, sample, path)


def experiment_summary(samples):
    results = []
    for size in ("2X-Small", "X-Small"):
        selected = [s for s in samples if s["size"] == size and s["status"] == "COMPLETE"]
        if not selected:
            continue
        priced = all(s.get("billing", {}).get("status") == "OBSERVED_RECORDS" for s in selected)
        results.append(
            {
                "size": size,
                "samples": len(selected),
                "median_queries_per_minute": statistics.median(
                    s["query_window"]["queries_per_minute"] for s in selected
                ),
                "median_lifecycle_seconds": statistics.median(
                    s["lifecycle_seconds"] for s in selected
                ),
                "queries": [
                    {
                        "query": query,
                        **{
                            key: statistics.median(
                                next(q[key] for q in s["summary"] if q["query"] == query)
                                for s in selected
                            )
                            for key in (
                                "median_client_elapsed_ms",
                                "median_engine_total_ms",
                                "median_engine_execution_ms",
                                "median_read_bytes",
                                "median_pruned_files",
                                "median_spill_bytes",
                            )
                        },
                    }
                    for query in sorted(q["query"] for q in selected[0]["summary"])
                ],
                "billing_status": "OBSERVED_PROVISIONAL" if priced else "PENDING_OR_INCOMPLETE",
                "median_full_sample_list_price_usd": (
                    statistics.median(
                        Decimal(str(s["billing"]["list_price_compute_estimate_usd"]))
                        for s in selected
                    )
                    if priced
                    else None
                ),
                "median_amortized_cost_per_measured_query_usd": (
                    statistics.median(
                        Decimal(str(s["billing"]["amortized_cost_per_measured_query_usd"]))
                        for s in selected
                    )
                    if priced
                    else None
                ),
            }
        )
    return results


def measure(client, experiment_id, duration_seconds, path):
    warehouse_name(experiment_id, "2xs_1")
    if duration_seconds <= 0:
        raise ValueError("Duration must be positive.")
    config = SnapshotConfig()
    reference = client.warehouses.get(COLLECTOR_ID)
    expected = {
        "cluster_size": "2X-Small",
        "warehouse_type": "PRO",
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "enable_photon": True,
        "enable_serverless_compute": True,
        "auto_stop_mins": 5,
        "channel": reference.channel.as_dict(),
    }
    if (
        reference.name != "nyc-taxi-dev-benchmark"
        or reference.state.value != "STOPPED"
        or reference.num_active_sessions
        or {key: reference.as_dict()[key] for key in WAREHOUSE_FIELDS} != expected
    ):
        raise ValueError("Reference/collector warehouse must be the stopped, unchanged baseline.")
    if path.exists():
        report = json.loads(path.read_text())
        if (
            report["experiment_id"] != experiment_id
            or report["duration_seconds"] != duration_seconds
            or report["workspace_host"] != client.config.host
            or report["workload"] != query_suite(config)
            or report["reference_warehouse"] != expected
            or report["software"]["source_sha256"] != software_versions()["source_sha256"]
            or report["software"]["warehouse_experiment_sha256"]
            != hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        ):
            raise ValueError("Existing experiment has different inputs, code or configuration.")
    else:
        report = {
            "experiment_id": experiment_id,
            "workspace_host": client.config.host,
            "remote_report": str(
                config.report_root
                / "warehouse_experiments"
                / experiment_id
                / "experiment_report.json"
            ),
            "duration_seconds": duration_seconds,
            "reference_warehouse": expected,
            "software": {
                **software_versions(),
                "warehouse_experiment_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
            },
            "workload": query_suite(config),
            "samples": [],
            "scope": (
                "Fresh warehouse per sample, used only for that sample's startup, metadata, "
                "one warmup per query, nominal fixed-time serial query window and shutdown. "
                "Complete four-query cycles finish even if the last crosses the deadline. "
                "Result cache disabled; physical I/O caches uncontrolled. No concurrency test. "
                "Per-size summaries are medians of three per-sample medians, not pooled queries. "
                "All warehouse usage, including warmup, setup and shutdown, belongs to the sample. "
                "Shutdown anomalies are retained and reported, not selectively discarded. "
                "SQL requests have a 30-second socket timeout and no retry sequence; trials "
                "exceeding the nominal query window by more than 60 wall-clock seconds fail. "
                "Billing is collected later on a different warehouse. Cost/query amortizes "
                "full-lifecycle cost over completed measured queries, not execution-only cost."
            ),
        }
    report["status"] = "RUNNING"
    report.pop("error", None)
    save_report(client, path, report)
    try:
        for plan in sample_plan():
            existing = next(
                (s for s in report["samples"] if s["sample_id"] == plan["sample_id"]), None
            )
            if existing:
                if existing["status"] not in ("MEASURED", "PROFILES_PENDING", "COMPLETE"):
                    raise ValueError("Partial sample will not be rerun automatically; inspect it.")
                if existing["status"] != "COMPLETE":
                    finish_sample(client, report, existing, path)
                continue
            LOGGER.info("Starting isolated %s (%s)", plan["sample_id"], plan["size"])
            measure_sample(client, report, plan, path, reference.channel)
            LOGGER.info("Completed isolated %s", plan["sample_id"])
        verify_results([m for s in report["samples"] for m in s["measurements"]])
        if len({digest(s["engine_version"]) for s in report["samples"]}) != 1:
            raise ValueError("Engine version changed across samples.")
        for sample in report["samples"]:
            warehouse = owned_warehouse(client, report, sample)
            if warehouse.state.value != "STOPPED" or warehouse.num_active_sessions:
                raise ValueError("Sample compute is not stopped and unused.")
        report["status"] = (
            "MEASURED_AND_VERIFIED"
            if all(s["status"] == "COMPLETE" for s in report["samples"])
            else "PROFILES_PENDING"
        )
        report["summary"] = experiment_summary(report["samples"])
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        if report["samples"] and report["samples"][-1]["status"] in ("CREATING", "RUNNING"):
            report["samples"][-1]["status"] = "FAILED"
        LOGGER.exception("Isolated SQL experiment failed; evidence retained.")
        raise
    finally:
        save_report(client, path, report)
    return report


def billing_totals(records, completed_queries):
    if completed_queries <= 0:
        raise ValueError("Billing denominator must be completed measured queries.")
    if len({r["record_id"] for r in records}) != len(records):
        raise ValueError("Duplicate billing record IDs; check effective price joins.")
    priced = bool(records) and all(r["list_price_cost_usd"] is not None for r in records)
    cost = (
        sum((Decimal(str(r["list_price_cost_usd"])) for r in records), Decimal(0))
        if priced
        else None
    )
    return {
        "status": "OBSERVED_RECORDS" if priced else ("UNPRICED" if records else "PENDING"),
        "usage_records": len(records),
        "dbu_quantity": (
            sum((Decimal(str(r["usage_quantity"])) for r in records), Decimal(0))
            if records
            else None
        ),
        "list_price_compute_estimate_usd": cost,
        "amortized_cost_per_measured_query_usd": (
            cost / completed_queries if cost is not None else None
        ),
    }


def refresh_billing(client, report, path):
    from databricks import sql

    if report["status"] != "MEASURED_AND_VERIFIED":
        raise ValueError("Billing requires a complete, verified experiment.")
    for sample in report["samples"]:
        warehouse = owned_warehouse(client, report, sample)
        if warehouse.id == COLLECTOR_ID or warehouse.state.value != "STOPPED":
            raise ValueError("Sample warehouse must be stopped and distinct from the collector.")
    collector = client.warehouses.get(COLLECTOR_ID)
    if collector.name != "nyc-taxi-dev-benchmark":
        raise ValueError("Use only the dedicated NYC taxi billing collector.")
    if collector.state.value != "STOPPED" or collector.num_active_sessions:
        raise ValueError("Billing collector is not stopped and unused.")
    try:
        client.warehouses.start(COLLECTOR_ID).result(timeout=timedelta(minutes=10))
        with (
            sql.connect(
                server_hostname=urlparse(client.config.host).hostname,
                http_path=collector.odbc_params.path,
                credentials_provider=lambda: client.config.authenticate,
                user_agent_entry="nyc-taxi-isolated-sql-billing",
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SET TIME ZONE 'UTC'")
            for sample in report["samples"]:
                records = query_rows(
                    cursor,
                    """
                    SELECT u.record_id, u.record_type, u.usage_start_time, u.usage_end_time,
                           u.sku_name, u.usage_unit, u.usage_quantity,
                           p.pricing.effective_list.default AS list_price_usd_per_dbu,
                           u.usage_quantity * p.pricing.effective_list.default
                             AS list_price_cost_usd
                    FROM system.billing.usage u
                    LEFT JOIN system.billing.list_prices p
                      ON u.sku_name=p.sku_name AND u.cloud=p.cloud
                     AND u.usage_unit=p.usage_unit AND p.currency_code='USD'
                     AND u.usage_start_time>=p.price_start_time
                     AND (p.price_end_time IS NULL OR u.usage_start_time<p.price_end_time)
                    WHERE u.usage_metadata.warehouse_id=:warehouse_id
                      AND u.cloud='AZURE' AND u.usage_unit='DBU'
                      AND u.usage_date BETWEEN :first_date AND :last_date
                    ORDER BY u.usage_start_time, u.record_id
                    """,
                    {
                        "warehouse_id": sample["warehouse_id"],
                        "first_date": datetime.fromisoformat(sample["lifecycle_started_at"]).date(),
                        "last_date": datetime.now(UTC).date(),
                    },
                )
                sample["billing"] = {
                    **billing_totals(records, sample["query_window"]["completed_measured_queries"]),
                    "collected_at": now(),
                    "records": records,
                }
        report["billing_scope"] = (
            "All usage on each fresh sample warehouse, including signed corrections, priced at "
            "effective USD list prices. No hourly-window overlap attribution or time proration. "
            "Includes startup, warmup, metadata and shutdown; excludes separate collector usage. "
            "Amortized cost/query divides whole-sample cost by completed measured queries. "
            "Observed records may still be incomplete and later corrections remain possible. "
            "Not an invoice; excludes discounts, credits, taxes and storage."
        )
        report["summary"] = experiment_summary(report["samples"])
        save_report(client, path, report)
    finally:
        stop_warehouse(client, COLLECTOR_ID)
        report["collector_stopped_at"] = now()
        save_report(client, path, report)


def main():
    parser = argparse.ArgumentParser(description="Isolated, timed SQL warehouse cost experiment.")
    parser.add_argument("--phase", choices=("measure", "billing"), required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--duration-seconds", type=int, default=600)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--auth-type", choices=("azure-cli", "databricks-cli"), default="azure-cli")
    args = parser.parse_args()
    warehouse_name(args.experiment_id, "2xs_1")
    from databricks.sdk import WorkspaceClient

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("databricks.sql").setLevel(logging.WARNING)
    client = WorkspaceClient(auth_type=args.auth_type)
    path = args.report or Path("benchmark-results") / f"{args.experiment_id}.json"
    if args.phase == "measure":
        measure(client, args.experiment_id, args.duration_seconds, path)
    else:
        report = json.loads(path.read_text())
        if (
            report["experiment_id"] != args.experiment_id
            or report["workspace_host"] != client.config.host
        ):
            raise ValueError("Billing report belongs to a different experiment/workspace.")
        refresh_billing(client, report, path)


if __name__ == "__main__":
    main()
