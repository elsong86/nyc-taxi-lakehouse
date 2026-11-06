import argparse
import hashlib
import io
import json
import logging
import re
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

from nyc_taxi_lakehouse.benchmark import frozen_versions, now, query_rows, software_versions
from nyc_taxi_lakehouse.benchmark_snapshot import SnapshotConfig
from nyc_taxi_lakehouse.layout_experiment import (
    BASE,
    audit_and_measure,
    check_versions,
    connection,
    detail,
    layout_queries,
    prepare_layout,
    save,
    stop_warehouse,
    target_table,
)
from nyc_taxi_lakehouse.warehouse_experiment import COLLECTOR_ID

LOGGER = logging.getLogger(__name__)
ANNUAL = SnapshotConfig(dataset_id="fy_2024", start_month="2024-01", end_month="2024-12")


def annual_counts(rows):
    expected = [f"2024-{month:02d}" for month in range(1, 13)]
    if sorted(row["source_month"] for row in rows) != expected:
        raise ValueError("Annual input must contain exactly January-December 2024.")
    if any(row["rows"] <= 0 for row in rows):
        raise ValueError("Every annual source month must contain rows.")
    return sum(row["rows"] for row in rows)


def clustering_on_write_evidence(history):
    for row in history:
        if row["operation"] != "CREATE TABLE AS SELECT":
            continue
        parameters = row["operationParameters"]
        if isinstance(parameters, str):
            match = re.search(r'"clusteringOnWriteStatus":"([^"]*)"', parameters)
            status = match.group(1) if match else None
        else:
            status = parameters.get("clusteringOnWriteStatus")
        metrics = row["operationMetrics"]
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        files = int(metrics["numFiles"]) if metrics.get("numFiles") is not None else None
        size = int(metrics["numOutputBytes"]) if metrics.get("numOutputBytes") is not None else None
        return {
            "status": status,
            "data_files_written": files,
            "data_bytes_written": size,
            "confirmed": (
                status == "kdtree triggered"
                and files is not None
                and files > 0
                and size is not None
                and size > 0
            ),
        }
    return {"status": None, "confirmed": False}


def clustering_gate(effects, history=()):
    rewrite = (
        effects.get("clustering_tasks_planned") is not None
        and effects["clustering_tasks_planned"] > 0
        and effects["data_files_added"] > 0
        and effects["data_files_removed"] > 0
        and effects["data_bytes_added"] > 0
        and effects["data_bytes_removed"] > 0
    )
    return rewrite or clustering_on_write_evidence(history)["confirmed"]


def exact_audit(cursor, source, target, expected_rows):
    differences = query_rows(
        cursor,
        "SELECT count(*) AS differences FROM ("
        f"(SELECT * FROM {source} EXCEPT ALL SELECT * FROM {target}) UNION ALL "
        f"(SELECT * FROM {target} EXCEPT ALL SELECT * FROM {source}))",
    )[0]["differences"]
    count = query_rows(cursor, f"SELECT count(*) AS rows FROM {target}")[0]["rows"]
    if differences or count != expected_rows:
        raise ValueError(f"Exact row-multiset audit failed for {target}.")
    return {"status": "PASSED", "rows": count, "differences": differences}


def archive_sources(client, report):
    root = Path(__file__).parent
    sources = {}
    remote_root = str(Path(report["remote_report"]).parent / "measured_sources")
    for name in (
        "layout_preflight.py",
        "layout_experiment.py",
        "benchmark.py",
        "benchmark_snapshot.py",
        "warehouse_experiment.py",
    ):
        payload = (root / name).read_bytes()
        remote = f"{remote_root}/{name}"
        client.files.upload(remote, io.BytesIO(payload), overwrite=False)
        sources[name] = {"sha256": hashlib.sha256(payload).hexdigest(), "remote_path": remote}
    report["measured_sources"] = sources


def run(client, experiment_id, path):
    target_table(experiment_id, "partitioned")
    if path.exists():
        raise ValueError("Preflight report exists; inspect saved evidence rather than rerun.")
    reference = client.warehouses.get(COLLECTOR_ID)
    if (
        reference.name != "nyc-taxi-dev-benchmark"
        or reference.state.value != "STOPPED"
        or reference.num_active_sessions
        or reference.cluster_size != "2X-Small"
        or reference.min_num_clusters != 1
        or reference.max_num_clusters != 1
        or not reference.enable_photon
        or not reference.enable_serverless_compute
    ):
        raise ValueError("Preflight requires the owned, stopped and unchanged 2X-Small collector.")
    report = {
        "experiment_id": experiment_id,
        "status": "RUNNING",
        "started_at": now(),
        "remote_report": str(
            BASE.report_root / "layout_preflights" / experiment_id / "experiment_report.json"
        ),
        "software": software_versions(),
        "reference_warehouse": reference.as_dict(),
        "layouts": [],
        "measurements": [],
        "scope": (
            "Annual Silver-only frozen materialization, January-December 2024 source months. "
            "Live source and manifest versions pinned; exact duplicate-preserving audits. "
            "Fresh dedicated preparation warehouse per layout; separate collector for freezing "
            "and audits. Common ZSTD/64 MiB best-effort target and source_month,pickup_at keys. "
            "Physical clustering gate requires explicit OPTIMIZE clustering tasks and data "
            "file/byte rewrites, or CTAS clustering-on-write confirmation with output files/bytes. "
            "No query repetitions, production changes, or Gold crossover."
        ),
    }
    save(client, path, report)
    try:
        archive_sources(client, report)
        save(client, path, report)
        try:
            client.warehouses.start(reference.id).result(timeout=timedelta(minutes=10))
            with connection(client, client.warehouses.get(reference.id)) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SET TIME ZONE 'UTC'")
                    catalog = query_rows(cursor, f"DESCRIBE CATALOG EXTENDED {BASE.catalog}")
                    predictive = [
                        row
                        for row in catalog
                        if any("predictive" in str(v).lower() for v in row.values())
                    ]
                    if not any(
                        "disable" in str(v).lower() for row in predictive for v in row.values()
                    ):
                        raise ValueError("Catalog predictive optimization must be disabled.")
                    report["catalog_description"] = catalog
                    targets = [ANNUAL.table("trips")] + [
                        target_table(experiment_id, layout) for layout in ("partitioned", "liquid")
                    ]
                    for table in targets:
                        occupied = query_rows(
                            cursor,
                            "SELECT count(*) AS tables FROM "
                            f"{BASE.catalog}.information_schema.tables "
                            "WHERE table_schema='benchmark' AND table_name=:name",
                            {"name": table.rsplit(".", 1)[1]},
                        )[0]["tables"]
                        if occupied:
                            raise ValueError(f"Target already exists; refusing overwrite: {table}")
                    q1 = query_rows(
                        cursor,
                        f"SELECT status,metadata_json FROM {BASE.catalog}.benchmark.datasets "
                        "WHERE dataset_id='q1_2024'",
                    )
                    if len(q1) != 1 or q1[0]["status"] != "READY":
                        raise ValueError("Q1 dataset must be uniquely READY.")
                    report["snapshot_versions"] = json.loads(q1[0]["metadata_json"])[
                        "snapshot_versions"
                    ]
                    check_versions(cursor, report)
                    source = f"{BASE.catalog}.silver.trips"
                    manifest = f"{BASE.catalog}.ops.file_manifest"
                    report["source_versions"] = frozen_versions(
                        cursor, {"snapshot_versions": {source: 0, manifest: 0}}
                    )
                    version = report["source_versions"][source]
                    manifest_version = report["source_versions"][manifest]
                    predicate = "source_month BETWEEN '2024-01' AND '2024-12'"
                    files = query_rows(
                        cursor,
                        f"SELECT * FROM {manifest} VERSION AS OF {manifest_version} "
                        f"WHERE {predicate} ORDER BY source_month",
                    )
                    annual_counts(
                        [
                            {"source_month": row["source_month"], "rows": row["valid_rows"]}
                            for row in files
                        ]
                    )
                    if any(row["status"] != "COMPLETE" or not row["sha256"] for row in files):
                        raise ValueError(
                            "Annual source manifests must be complete and checksummed."
                        )
                    report["source_files"] = files
                    source_query = f"{source} VERSION AS OF {version}"
                    counts = query_rows(
                        cursor,
                        f"SELECT source_month,count(*) AS rows FROM {source_query} "
                        f"WHERE {predicate} GROUP BY source_month ORDER BY source_month",
                    )
                    report["expected_rows"] = annual_counts(counts)
                    if {r["source_month"]: r["rows"] for r in counts} != {
                        r["source_month"]: r["valid_rows"] for r in files
                    }:
                        raise ValueError(
                            "Pinned Silver counts do not reconcile with source manifests."
                        )
                    report["source_month_counts"] = counts
                    save(client, path, report)
                    cursor.execute(
                        f"CREATE TABLE {ANNUAL.table('trips')} USING DELTA "
                        "PARTITIONED BY (source_month) "
                        "TBLPROPERTIES ('delta.parquet.compression.codec'='zstd') "
                        f"AS SELECT * FROM {source_query} WHERE {predicate}"
                    )
                    cursor.fetchall()
                    report["freeze_statement_id"] = cursor.query_id
                    report["snapshot_versions"][ANNUAL.table("trips")] = query_rows(
                        cursor, f"DESCRIBE HISTORY {ANNUAL.table('trips')} LIMIT 1"
                    )[0]["version"]
                    report["baseline_detail"] = detail(cursor, ANNUAL.table("trips"))
                    report["freeze_audit"] = exact_audit(
                        cursor,
                        f"(SELECT * FROM {source_query} WHERE {predicate})",
                        ANNUAL.table("trips"),
                        report["expected_rows"],
                    )
                    check_versions(cursor, report)
                    save(client, path, report)
        finally:
            stop_warehouse(client, reference.id)
            report["freeze_collector_stopped_at"] = now()
            save(client, path, report)
        for layout in ("partitioned", "liquid"):
            prepare_layout(client, report, layout, reference, path, ANNUAL)
        report["target_versions"] = {item["table"]: item["version"] for item in report["layouts"]}
        try:
            client.warehouses.start(reference.id).result(timeout=timedelta(minutes=10))
            with connection(client, client.warehouses.get(reference.id)) as conn:
                with conn.cursor() as cursor:
                    check_versions(cursor, report)
                    report["engine_version"] = query_rows(
                        cursor, "SELECT current_version() AS version"
                    )
                    for item in report["layouts"]:
                        item["audit"] = exact_audit(
                            cursor, ANNUAL.table("trips"), item["table"], report["expected_rows"]
                        )
                        save(client, path, report)
                    check_versions(cursor, report)
        finally:
            stop_warehouse(client, reference.id)
            report["audit_collector_stopped_at"] = now()
        liquid = next(item for item in report["layouts"] if item["layout"] == "liquid")
        liquid["clustering_on_write"] = clustering_on_write_evidence(liquid["history"])
        report["physical_clustering_status"] = (
            "VERIFIED"
            if clustering_gate(liquid["optimize_effects"], liquid["history"])
            else "NOT_VERIFIED"
        )
        report["status"] = "PREFLIGHT_VERIFIED"
        report["completed_at"] = now()
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        LOGGER.exception("Annual preflight failed; partial evidence preserved.")
        raise
    finally:
        save(client, path, report)
    return report


def enrich(client, report, path):
    if report["status"] != "PREFLIGHT_VERIFIED":
        raise ValueError("Physical evidence enrichment requires a completed, audited preflight.")
    reference = client.warehouses.get(COLLECTOR_ID)
    if (
        reference.name != "nyc-taxi-dev-benchmark"
        or reference.state.value != "STOPPED"
        or reference.num_active_sessions
    ):
        raise ValueError("Physical evidence collector must be owned, stopped and unused.")
    for item in report["layouts"]:
        warehouse = client.warehouses.get(item["warehouse_id"])
        if (
            warehouse.name != item["warehouse_name"]
            or warehouse.state.value != "STOPPED"
            or warehouse.num_active_sessions
        ):
            raise ValueError("Preparation warehouses must be owned, stopped and unused.")
    payload = Path(__file__).read_bytes()
    remote = str(Path(report["remote_report"]).parent / "post_run_collectors/layout_preflight.py")
    client.files.upload(remote, io.BytesIO(payload), overwrite=True)
    report["post_run_collector"] = {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "remote_path": remote,
    }
    try:
        client.warehouses.start(reference.id).result(timeout=timedelta(minutes=10))
        with connection(client, client.warehouses.get(reference.id)) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SET use_cached_result = false")
                check_versions(cursor, report)
                for item in report["layouts"]:
                    ranges = query_rows(
                        cursor,
                        "SELECT _metadata.file_name AS file_name, "
                        "max(_metadata.file_size) AS file_bytes, count(*) AS rows, "
                        "min(source_month) AS min_source_month, "
                        "max(source_month) AS max_source_month, "
                        "min(pickup_at) AS min_pickup_at, max(pickup_at) AS max_pickup_at "
                        f"FROM {item['table']} GROUP BY _metadata.file_name "
                        "ORDER BY file_name",
                    )
                    if (
                        sum(row["rows"] for row in ranges) != report["expected_rows"]
                        or len(ranges) != item["after_optimize"]["num_files"]
                        or sum(row["file_bytes"] for row in ranges)
                        != item["after_optimize"]["active_data_bytes"]
                    ):
                        raise ValueError(
                            "Observed file ranges do not reconcile with layout detail."
                        )
                    item["file_range_statistics"] = ranges
                    item["file_range_statement_id"] = cursor.query_id
                    save(client, path, report)
                check_versions(cursor, report)
        liquid = next(item for item in report["layouts"] if item["layout"] == "liquid")
        liquid["clustering_on_write"] = clustering_on_write_evidence(liquid["history"])
        report.setdefault(
            "original_physical_clustering_status", report["physical_clustering_status"]
        )
        report["physical_clustering_status"] = (
            "VERIFIED"
            if clustering_gate(liquid["optimize_effects"], liquid["history"])
            else "NOT_VERIFIED"
        )
        method = "CTAS_ON_WRITE" if liquid["clustering_on_write"]["confirmed"] else "OPTIMIZE"
        report["physical_clustering_method"] = (
            method if report["physical_clustering_status"] == "VERIFIED" else None
        )
        report["post_run_scope"] = (
            "Read-only per-file row/range statistics and recorded CTAS history enrichment; "
            "no data rewrites or performance repetitions. The original gate considered only "
            "OPTIMIZE rewrites. CTAS kdtree-triggered status plus positive output file/byte "
            "counters confirms clustering on write, even when subsequent OPTIMIZE is a no-op. "
            "Original measured sources and initial gate result retained."
        )
        report["physical_evidence_collected_at"] = now()
    finally:
        stop_warehouse(client, reference.id)
        report["physical_evidence_collector_stopped_at"] = now()
        save(client, path, report)


def validate_preflight(preflight):
    experiment_id = preflight["experiment_id"]
    if (
        preflight["status"] != "PREFLIGHT_VERIFIED"
        or preflight["physical_clustering_status"] != "VERIFIED"
    ):
        raise ValueError("Query comparison requires verified physical clustering and exact audits.")
    if {item["layout"] for item in preflight["layouts"]} != {"partitioned", "liquid"} or len(
        preflight["layouts"]
    ) != 2:
        raise ValueError("Query comparison requires exactly two comparable layouts.")
    for item in preflight["layouts"]:
        if (
            item["table"] != target_table(experiment_id, item["layout"])
            or item["audit"]["status"] != "PASSED"
            or item["audit"]["differences"] != 0
            or item["audit"]["rows"] != preflight["expected_rows"]
            or preflight["target_versions"].get(item["table"]) != item["version"]
        ):
            raise ValueError("Preflight layout identity, version or exact audit is inconsistent.")
    liquid = next(item for item in preflight["layouts"] if item["layout"] == "liquid")
    if not clustering_gate(liquid["optimize_effects"], liquid["history"]):
        raise ValueError("Preflight has no supporting physical clustering evidence.")


def annual_layout_queries(experiment_id, layout):
    queries = layout_queries(experiment_id, layout)
    for query in queries:
        if query["name"] == "01_monthly_fares":
            query["sql"] = query["sql"].replace(
                "avg(trip_distance_miles)",
                "avg(CAST(trip_distance_miles AS DECIMAL(28,6)))",
            )
        query["sql_sha256"] = hashlib.sha256(query["sql"].encode()).hexdigest()
    return queries


def measure_queries(client, preflight_path, path):
    if path.exists():
        raise ValueError("Query report exists; preserve measurements rather than rerun.")
    payload = preflight_path.read_bytes()
    preflight = json.loads(payload)
    validate_preflight(preflight)
    reference = client.warehouses.get(COLLECTOR_ID)
    if (
        reference.name != "nyc-taxi-dev-benchmark"
        or reference.state.value != "STOPPED"
        or reference.num_active_sessions
        or reference.cluster_size != "2X-Small"
        or reference.min_num_clusters != 1
        or reference.max_num_clusters != 1
        or not reference.enable_photon
        or not reference.enable_serverless_compute
    ):
        raise ValueError(
            "Query comparison requires the owned, stopped, unchanged 2X-Small warehouse."
        )
    experiment_id = preflight["experiment_id"]
    report = {
        "experiment_id": experiment_id,
        "status": "RUNNING",
        "started_at": now(),
        "remote_report": str(
            BASE.report_root / "layout_preflights" / experiment_id / "query_report_decimal.json"
        ),
        "preflight_report": preflight["remote_report"],
        "preflight_report_sha256": hashlib.sha256(payload).hexdigest(),
        "software": software_versions(),
        "reference_warehouse": reference.as_dict(),
        "snapshot_versions": deepcopy(preflight["snapshot_versions"]),
        "target_versions": deepcopy(preflight["target_versions"]),
        "expected_rows": preflight["expected_rows"],
        "layouts": deepcopy(preflight["layouts"]),
        "measurements": [],
        "queries": {
            layout: annual_layout_queries(experiment_id, layout)
            for layout in ("partitioned", "liquid")
        },
        "decimal_distance_average": True,
        "physical_clustering_status": preflight["physical_clustering_status"],
        "physical_clustering_method": preflight.get("physical_clustering_method"),
        "scope": (
            "Read-only query comparison of the exact annual preflight layouts, no new writes. "
            "Same stopped/unused 2X-Small serverless Photon warehouse for both. Three interleaved "
            "blocks per layout, one warmup plus three measured repeats per query/block. "
            "Monthly fares and airport corridors scan annual Silver; weekly demand retains "
            "January 1-8 predicates to test selective skipping. Frozen Q1 Gold is an unchanged "
            "timing control. Monthly distance average uses DECIMAL(28,6) after a full-source "
            "round-trip precision check, avoiding layout-dependent DOUBLE rounding ties. "
            "No annual Gold. Exact EXCEPT ALL audits and version guards. "
            "Result caching disabled and checked in every final profile; physical I/O caches "
            "not cleared. Warm-cache serial workload, not concurrency/cold-cache testing. "
            "No per-layout query-serving cost attribution on shared compute."
        ),
    }
    save(client, path, report)
    try:
        # Keep the query archive separate from the immutable preparation sources.
        source_report = {
            "remote_report": str(
                Path(report["remote_report"]).parent / "query_sources_decimal" / "report.json"
            )
        }
        archive_sources(client, source_report)
        report["measured_sources"] = source_report["measured_sources"]
        for source in sorted((Path(__file__).parent / "benchmark_sql").glob("*.sql")):
            data = source.read_bytes()
            name = f"benchmark_sql/{source.name}"
            remote = str(Path(source_report["remote_report"]).parent / "measured_sources" / name)
            client.files.upload(remote, io.BytesIO(data), overwrite=False)
            report["measured_sources"][name] = {
                "sha256": hashlib.sha256(data).hexdigest(),
                "remote_path": remote,
            }
        save(client, path, report)
        audit_and_measure(client, report, reference, path, ANNUAL)
        if len(report["measurements"]) != 96 or any(
            m.get("profile", {}).get("metrics", {}).get("result_from_cache") is not False
            for m in report["measurements"]
        ):
            raise ValueError("Expected 96 complete, explicitly uncached query profiles.")
        report["completed_at"] = now()
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        LOGGER.exception("Annual query comparison failed; measurements preserved without reruns.")
        raise
    finally:
        save(client, path, report)
    return report


def main():
    parser = argparse.ArgumentParser(description="Annual layout physical-clustering preflight.")
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--phase", choices=("prepare", "enrich", "queries"), default="prepare")
    args = parser.parse_args()
    from databricks.sdk import WorkspaceClient

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("databricks.sql").setLevel(logging.WARNING)
    target_table(args.experiment_id, "partitioned")
    client = WorkspaceClient(auth_type="azure-cli")
    path = Path("benchmark-results") / f"{args.experiment_id}.json"
    if args.phase == "prepare":
        run(client, args.experiment_id, path)
    elif args.phase == "queries":
        measure_queries(client, path, path.with_stem(f"{args.experiment_id}_queries_decimal"))
    else:
        report = json.loads(path.read_text())
        if report["experiment_id"] != args.experiment_id:
            raise ValueError("Report belongs to a different preflight.")
        enrich(client, report, path)


if __name__ == "__main__":
    main()
