import argparse
import hashlib
import io
import json
import logging
import platform
import statistics
import subprocess
import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from importlib import metadata, resources
from pathlib import Path
from urllib.parse import urlparse

from nyc_taxi_lakehouse.benchmark_snapshot import SnapshotConfig, assert_frozen_versions

LOGGER = logging.getLogger(__name__)


def now() -> str:
    return datetime.now(UTC).isoformat()


def digest(value) -> str:
    return hashlib.sha256(
        json.dumps(value, default=str, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def query_suite(config: SnapshotConfig) -> list[dict]:
    directory = resources.files("nyc_taxi_lakehouse").joinpath("benchmark_sql")
    tables = {
        "trips": config.table("trips"),
        "hourly_zone_demand": config.table("hourly_zone_demand"),
    }
    queries = []
    for path in sorted(directory.iterdir(), key=lambda item: item.name):
        if path.name.endswith(".sql"):
            template = path.read_text()
            queries.append(
                {
                    "name": path.name.removesuffix(".sql"),
                    "template_sha256": hashlib.sha256(template.encode()).hexdigest(),
                    "sql": template.format(**tables),
                }
            )
    if not queries:
        raise ValueError("No packaged benchmark queries found.")
    return queries


def result_digest(columns: list[str], rows: list) -> str:
    return digest({"columns": columns, "rows": [list(row) for row in rows]})


def verify_results(measurements: list[dict]):
    signatures = {}
    for item in measurements:
        name = item["query"]
        signature = (item["result_sha256"], item["result_rows"])
        if name in signatures and signatures[name] != signature:
            raise ValueError(f"Query results changed across repetitions: {name}")
        signatures[name] = signature


def summarize(measurements: list[dict]) -> list[dict]:
    verify_results(measurements)
    summaries = []
    for name in sorted({item["query"] for item in measurements}):
        samples = [item for item in measurements if item["query"] == name and not item["warmup"]]
        if not samples:
            raise ValueError(f"No measured repetitions for {name}.")
        profiles = [
            item["profile"]["metrics"] for item in samples if item.get("profile", {}).get("metrics")
        ]

        def median_metric(key):
            values = [profile[key] for profile in profiles if profile.get(key) is not None]
            return statistics.median(values) if values else None

        durations = [item["client_elapsed_ms"] for item in samples]
        summaries.append(
            {
                "query": name,
                "samples": len(samples),
                "profile_samples": len(profiles),
                "result_rows": samples[0]["result_rows"],
                "result_sha256": samples[0]["result_sha256"],
                "median_client_elapsed_ms": statistics.median(durations),
                "min_client_elapsed_ms": min(durations),
                "max_client_elapsed_ms": max(durations),
                "median_engine_total_ms": median_metric("total_time_ms"),
                "median_engine_execution_ms": median_metric("execution_time_ms"),
                "median_read_bytes": median_metric("read_bytes"),
                "median_read_files": median_metric("read_files_count"),
                "median_pruned_files": median_metric("pruned_files_count"),
                "median_spill_bytes": median_metric("spill_to_disk_bytes"),
                "median_cache_read_bytes": median_metric("read_cache_bytes"),
                "median_remote_read_bytes": median_metric("read_remote_bytes"),
            }
        )
    return summaries


def query_rows(cursor, statement: str, parameters=None) -> list[dict]:
    cursor.execute(statement, parameters=parameters)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def frozen_versions(cursor, snapshot: dict) -> dict:
    return {
        table: int(query_rows(cursor, f"DESCRIBE HISTORY {table} LIMIT 1")[0]["version"])
        for table in snapshot["snapshot_versions"]
    }


def collect_profiles(client, measurements: list[dict], timeout_seconds: int = 90) -> list[str]:
    from databricks.sdk.service.sql import QueryFilter

    pending = {item["statement_id"] for item in measurements}
    profiles = {}
    deadline = time.monotonic() + timeout_seconds
    while pending:
        page_token = None
        while True:
            response = client.query_history.list(
                filter_by=QueryFilter(statement_ids=sorted(pending)),
                include_metrics=True,
                max_results=100,
                page_token=page_token,
            )
            for query in response.res or []:
                if query.query_id in pending and query.is_final and query.metrics is not None:
                    profile = query.as_dict()
                    if profile["metrics"].get("result_from_cache") is True:
                        raise ValueError(f"Result cache was used for statement {query.query_id}.")
                    profiles[query.query_id] = profile
            page_token = response.next_page_token
            if not page_token:
                break
        pending -= profiles.keys()
        if not pending or time.monotonic() >= deadline:
            break
        time.sleep(3)
    for item in measurements:
        if item["statement_id"] in profiles:
            item["profile"] = profiles[item["statement_id"]]
    return sorted(pending)


def collect_billing(cursor, catalog: str, job_id: str, warehouse_id: str, window: dict) -> dict:
    lookback_start = datetime.fromisoformat(window["started_at"]) - timedelta(days=7)
    window_end = datetime.fromisoformat(window["ended_at"])
    contexts = query_rows(
        cursor,
        f"SELECT job_run_id, min(started_at) AS started_at, "
        "sum(CASE WHEN status = 'SUCCEEDED' THEN processed_rows ELSE 0 END) AS processed_rows, "
        "concat_ws(',', sort_array(collect_set(status))) AS processing_status "
        f"FROM {catalog}.ops.pipeline_runs "
        "WHERE started_at >= :lookback_start AND started_at <= :window_end "
        "GROUP BY job_run_id",
        {"lookback_start": lookback_start, "window_end": window_end},
    )
    price_query = """
        SELECT u.usage_metadata.job_run_id AS job_run_id,
               u.sku_name, u.usage_unit, count(*) AS usage_records,
               sum(u.usage_quantity) AS usage_quantity,
               sum(u.usage_quantity * p.pricing.effective_list.default) AS list_price_cost_usd,
               sum(CASE WHEN p.pricing.effective_list.default IS NULL THEN 1 ELSE 0 END)
                   AS unpriced_records
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
          ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
         AND p.currency_code = 'USD'
         AND u.usage_start_time >= p.price_start_time
         AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
        WHERE u.usage_date BETWEEN :first_date AND :last_date
          AND u.cloud = 'AZURE' AND u.usage_unit = 'DBU'
          AND {selector}
        GROUP BY u.usage_metadata.job_run_id, u.sku_name, u.usage_unit
    """
    pipeline = query_rows(
        cursor,
        price_query.format(selector="u.usage_metadata.job_id = :job_id"),
        {"job_id": job_id, "first_date": lookback_start.date(), "last_date": window_end.date()},
    )
    warehouse = query_rows(
        cursor,
        price_query.format(
            selector="u.usage_metadata.warehouse_id = :warehouse_id "
            "AND u.usage_start_time < :window_end AND u.usage_end_time > :window_start"
        ),
        {
            "warehouse_id": warehouse_id,
            "first_date": lookback_start.date(),
            "last_date": window_end.date(),
            "window_start": datetime.fromisoformat(window["started_at"]),
            "window_end": datetime.fromisoformat(window["ended_at"]),
        },
    )
    runs = []
    for context in contexts:
        usage = [row for row in pipeline if row["job_run_id"] == context["job_run_id"]]
        priced = bool(usage) and all(row["unpriced_records"] == 0 for row in usage)
        cost = (
            sum((Decimal(str(row["list_price_cost_usd"])) for row in usage), Decimal(0))
            if priced
            else None
        )
        runs.append(
            {
                **context,
                "usage": usage,
                "billing_status": "OBSERVED" if priced else ("UNPRICED" if usage else "PENDING"),
                "list_price_compute_estimate_usd": cost,
                "list_price_usd_per_million_processed_rows": (
                    cost * 1_000_000 / context["processed_rows"]
                    if cost is not None and context["processed_rows"]
                    else None
                ),
            }
        )
    return {
        "status": "OBSERVED_RECORDS" if pipeline or warehouse else "PENDING",
        "collected_at": now(),
        "window_queried": window.copy(),
        "pipeline_runs": runs,
        "warehouse_usage": warehouse,
        "warehouse_billing_status": "PROVISIONAL" if warehouse else "PENDING",
        "scope": (
            "Observed DBU usage priced at effective USD list prices, including signed corrections. "
            "Not an Azure invoice: excludes discounts, credits, taxes and storage. Warehouse "
            "records overlap the measurement window and may include startup, idle and metadata "
            "queries. Billing typically arrives within 12 hours; missing records are not zero cost."
        ),
    }


def billing_report(cursor, catalog: str, job_id: str, warehouse_id: str, window: dict) -> dict:
    from databricks.sql.exc import ServerOperationError

    try:
        return collect_billing(cursor, catalog, job_id, warehouse_id, window)
    except ServerOperationError as error:
        if not str(error).startswith("[INSUFFICIENT_PERMISSIONS]"):
            raise
        LOGGER.error(
            "Billing unavailable; an account administrator must grant read access: %s", error
        )
        return {
            "status": "UNAVAILABLE",
            "collected_at": now(),
            "error": str(error),
            "required_access": [
                "USE CATALOG on system",
                "USE SCHEMA on system.billing",
                "SELECT on system.billing.usage",
                "SELECT on system.billing.list_prices",
            ],
        }


def stop_warehouse(client, warehouse_id: str):
    for _ in range(3):
        client.warehouses.stop(warehouse_id).result(timeout=timedelta(minutes=5))
        time.sleep(2)
        if client.warehouses.get(warehouse_id).state.value == "STOPPED":
            return
    raise RuntimeError(f"Warehouse {warehouse_id} did not remain stopped after stop requests.")


def software_versions() -> dict:
    git = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], capture_output=True, text=True, check=True
    ).stdout.strip()
    package = resources.files("nyc_taxi_lakehouse")
    sources = {
        name: hashlib.sha256(package.joinpath(name).read_bytes()).hexdigest()
        for name in ("benchmark.py", "benchmark_snapshot.py", "transforms.py")
    }
    return {
        "python": platform.python_version(),
        "sql_connector": metadata.version("databricks-sql-connector"),
        "sdk": metadata.version("databricks-sdk"),
        "git_commit": git,
        "git_dirty": bool(dirty),
        "source_sha256": sources,
    }


def persist_report(client, config: SnapshotConfig, report: dict, output_directory: Path) -> Path:
    report["report_id"] = str(uuid.uuid4())
    report["collected_at"] = now()
    payload = json.dumps(report, default=str, indent=2, sort_keys=True).encode()
    output_directory.mkdir(parents=True, exist_ok=True)
    filename = f"{report['benchmark_run_id']}_{report['report_id']}.json"
    local = output_directory / filename
    local.write_bytes(payload)
    remote = str(config.report_root / filename)
    client.files.upload(remote, io.BytesIO(payload), overwrite=False)
    LOGGER.info("Report saved locally at %s and remotely at %s", local, remote)
    return local


def run_benchmark(
    client,
    warehouse_id: str,
    config: SnapshotConfig,
    job_id: str,
    repetitions: int,
    output_directory: Path,
    previous_report: dict | None = None,
) -> dict:
    from databricks import sql

    if repetitions < 1:
        raise ValueError("At least one measured repetition is required.")
    warehouse = client.warehouses.get(warehouse_id)
    if not (warehouse.name.startswith("nyc-taxi-") and warehouse.name.endswith("-benchmark")):
        raise ValueError("Use a dedicated NYC taxi benchmark warehouse, not shared compute.")
    if warehouse.state.value != "STOPPED" or warehouse.num_active_sessions:
        raise ValueError("Benchmark warehouse must be stopped and unused before starting.")
    if warehouse.max_num_clusters != 1 or not warehouse.enable_serverless_compute:
        raise ValueError("Baseline requires a single-cluster serverless SQL warehouse.")
    report = previous_report or {
        "benchmark_run_id": str(uuid.uuid4()),
        "status": "RUNNING",
        "workspace_host": client.config.host,
        "warehouse_id": warehouse_id,
        "pipeline_job_id": job_id,
        "catalog": config.catalog,
        "dataset_id": config.dataset_id,
        "software": software_versions(),
        "warehouse": {
            key: value
            for key, value in warehouse.as_dict().items()
            if key
            in (
                "id",
                "name",
                "cluster_size",
                "min_num_clusters",
                "max_num_clusters",
                "enable_photon",
                "enable_serverless_compute",
                "auto_stop_mins",
                "channel",
            )
        },
        "measurements": [],
    }
    refresh = previous_report is not None
    if refresh:
        report["refresh_software"] = software_versions()
        report.pop("error", None)
        report.pop("cleanup_status", None)
    try:
        session_start = now()
        client.warehouses.start(warehouse_id).result(timeout=timedelta(minutes=10))
        if not refresh:
            report["measurement_window"] = {"started_at": session_start, "ended_at": None}
        with (
            sql.connect(
                server_hostname=urlparse(client.config.host).hostname,
                http_path=warehouse.odbc_params.path,
                credentials_provider=lambda: client.config.authenticate,
                user_agent_entry="nyc-taxi-benchmark",
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute("SET use_cached_result = false")
            setting = query_rows(cursor, "SET use_cached_result")
            if not setting or str(next(reversed(setting[0].values()))).lower() != "false":
                raise ValueError("Could not verify that SQL result caching is disabled.")
            datasets = query_rows(
                cursor,
                f"SELECT status, metadata_json FROM {config.catalog}.benchmark.datasets "
                "WHERE dataset_id = :dataset_id",
                {"dataset_id": config.dataset_id},
            )
            if len(datasets) != 1 or datasets[0]["status"] != "READY":
                raise ValueError("Benchmark dataset is not uniquely registered and READY.")
            snapshot = json.loads(datasets[0]["metadata_json"])
            if refresh:
                assert_frozen_versions(
                    report["dataset"]["snapshot_versions"], snapshot["snapshot_versions"]
                )
            if snapshot["start_month"] != "2024-01" or snapshot["end_month"] != "2024-03":
                raise ValueError("This fixed query suite is defined for January–March 2024.")
            assert_frozen_versions(snapshot["snapshot_versions"], frozen_versions(cursor, snapshot))
            if not refresh:
                report["dataset"] = snapshot
                report["engine_version"] = query_rows(cursor, "SELECT current_version() AS version")
                report["cache_policy"] = (
                    "SQL result cache disabled in the shared SQL session. One warmup per query "
                    "excluded from summaries. I/O cache is not cleared; this is a warm-cache "
                    "baseline, not a cold-cache experiment."
                )
                queries = query_suite(config)
                report["workload"] = queries
                report["workload_sha256"] = digest(
                    [(query["name"], query["template_sha256"]) for query in queries]
                )
                for query in queries:
                    for iteration in range(repetitions + 1):
                        started_at = now()
                        started = time.perf_counter()
                        cursor.execute(query["sql"])
                        rows = cursor.fetchall()
                        elapsed_ms = (time.perf_counter() - started) * 1000
                        columns = [column[0] for column in cursor.description]
                        report["measurements"].append(
                            {
                                "query": query["name"],
                                "iteration": iteration,
                                "warmup": iteration == 0,
                                "statement_id": cursor.query_id,
                                "started_at": started_at,
                                "client_elapsed_ms": elapsed_ms,
                                "result_rows": len(rows),
                                "result_sha256": result_digest(columns, rows),
                            }
                        )
                        LOGGER.info(
                            "%s iteration %s: %.1fms, %s rows",
                            query["name"],
                            iteration,
                            elapsed_ms,
                            len(rows),
                        )
                verify_results(report["measurements"])
                report["measurement_window"]["ended_at"] = now()
            report["pending_statement_ids"] = collect_profiles(client, report["measurements"])
            report["summary"] = summarize(report["measurements"])
            report["billing"] = billing_report(
                cursor, config.catalog, job_id, warehouse_id, report["measurement_window"]
            )
            assert_frozen_versions(snapshot["snapshot_versions"], frozen_versions(cursor, snapshot))
            report["status"] = (
                "QUERY_PROFILES_PENDING" if report["pending_statement_ids"] else "QUERIES_MEASURED"
            )
            if report["billing"]["status"] == "UNAVAILABLE":
                report["status"] += "_BILLING_UNAVAILABLE"
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        LOGGER.exception("Benchmark collection failed.")
        raise
    finally:
        try:
            stop_warehouse(client, warehouse_id)
            report["warehouse_stopped_at"] = now()
            if not refresh and "measurement_window" in report:
                report["measurement_window"]["ended_at"] = report["warehouse_stopped_at"]
        except Exception:
            report["cleanup_status"] = "WAREHOUSE_STOP_FAILED"
            LOGGER.exception("Could not stop benchmark warehouse %s.", warehouse_id)
            raise
        finally:
            persist_report(client, config, report, output_directory)
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Measure frozen SQL workloads and observed DBU usage."
    )
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--pipeline-job-id")
    parser.add_argument("--catalog", default="nyc_taxi_dev")
    parser.add_argument("--dataset-id", default="q1_2024")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output-directory", type=Path, default=Path("benchmark-results"))
    parser.add_argument("--refresh-report", type=Path)
    args = parser.parse_args()
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(auth_type="databricks-cli")
    previous = json.loads(args.refresh_report.read_text()) if args.refresh_report else None
    if previous:
        if (
            previous["workspace_host"] != client.config.host
            or previous["warehouse_id"] != args.warehouse_id
        ):
            parser.error("Refresh report belongs to a different workspace or warehouse.")
        config = SnapshotConfig(previous["catalog"], previous["dataset_id"])
        job_id = previous["pipeline_job_id"]
    else:
        if not args.pipeline_job_id:
            parser.error("--pipeline-job-id is required for a new measurement.")
        config = SnapshotConfig(args.catalog, args.dataset_id)
        job_id = args.pipeline_job_id
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True
    )
    report = run_benchmark(
        client,
        args.warehouse_id,
        config,
        job_id,
        args.repetitions,
        args.output_directory,
        previous,
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "summary": report.get("summary"),
                "billing_status": report["billing"]["status"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
