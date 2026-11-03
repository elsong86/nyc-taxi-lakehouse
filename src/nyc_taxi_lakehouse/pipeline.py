import json
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F

from nyc_taxi_lakehouse.config import Config
from nyc_taxi_lakehouse.sources import (
    ZONE_URL,
    check_revision,
    discover,
    land_file,
    remote_metadata,
    select_months,
    session,
    trip_url,
    verified_landed_file,
)
from nyc_taxi_lakehouse.transforms import bronze_records, gold_tables, silver_records

LOGGER = logging.getLogger(__name__)
FILE_SCHEMA = (
    "source_month string, url string, content_length long, etag string, last_modified string, "
    "sha256 string, source_schema_json string, bronze_rows long, valid_rows long, "
    "quarantine_rows long, duplicate_candidate_rows long, status string, updated_at timestamp, "
    "run_id string"
)
RUN_SCHEMA = (
    "run_id string, job_run_id string, started_at timestamp, finished_at timestamp, status string, "
    "selected_months array<string>, downloaded_bytes long, processed_rows long, "
    "processed_source_bytes long, bronze_rows long, valid_rows long, quarantine_rows long, "
    "error_message string"
)
STAGE_SCHEMA = (
    "run_id string, stage string, elapsed_seconds double, status string, recorded_at timestamp"
)


def now():
    return datetime.now(UTC)


def initialize(spark, config: Config):
    for name, schema in (
        ("file_manifest", FILE_SCHEMA),
        ("pipeline_runs", RUN_SCHEMA),
        ("stage_metrics", STAGE_SCHEMA),
    ):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {config.table('ops', name)} ({schema}) USING DELTA")


def update_manifest(spark, config: Config, record: dict):
    spark.createDataFrame([record], FILE_SCHEMA).createOrReplaceTempView("taxi_manifest_update")
    spark.sql(
        f"MERGE INTO {config.table('ops', 'file_manifest')} target "
        "USING taxi_manifest_update source ON target.source_month = source.source_month "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


def replace_month(frame, table: str, month: str):
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"source_month = '{month}'")
        .option("mergeSchema", "true")
        .partitionBy("source_month")
        .saveAsTable(table)
    )


def validate_outputs(spark, config: Config) -> dict:
    bronze = spark.table(config.table("bronze", "trips")).count()
    valid = spark.table(config.table("silver", "trips")).count()
    quarantine = spark.table(config.table("silver", "quarantine")).count()
    if bronze != valid + quarantine:
        raise ValueError(
            f"Layer reconciliation failed: Bronze={bronze}, Silver={valid}, quarantine={quarantine}"
        )
    demand = (
        spark.table(config.table("gold", "hourly_zone_demand"))
        .agg(F.coalesce(F.sum("trip_count"), F.lit(0)).alias("trips"))
        .first()["trips"]
    )
    if demand != valid:
        raise ValueError(f"Gold demand total {demand} does not match Silver {valid}.")
    return {"bronze_rows": bronze, "valid_rows": valid, "quarantine_rows": quarantine}


def run(spark, config: Config, *, pinned_inputs: dict | None = None) -> dict:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    initialize(spark, config)
    run_id = str(uuid.uuid4())
    result = {
        "run_id": run_id,
        "job_run_id": config.job_run_id,
        "started_at": now(),
        "finished_at": None,
        "status": "RUNNING",
        "selected_months": [],
        "downloaded_bytes": 0,
        "processed_rows": 0,
        "processed_source_bytes": 0,
        "bronze_rows": None,
        "valid_rows": None,
        "quarantine_rows": None,
        "error_message": None,
    }
    stages = []

    def stage(name, operation):
        start = time.perf_counter()
        status = "FAILED"
        try:
            value = operation()
            status = "SUCCEEDED"
            return value
        finally:
            elapsed = time.perf_counter() - start
            LOGGER.info("Stage %s: %s in %.2fs", name, status, elapsed)
            stages.append(
                {
                    "run_id": run_id,
                    "stage": name,
                    "elapsed_seconds": elapsed,
                    "status": status,
                    "recorded_at": now(),
                }
            )

    try:
        with session() as client:
            previous = {
                row["source_month"]: row.asDict()
                for row in spark.table(config.table("ops", "file_manifest")).collect()
            }
            months = stage(
                "discover",
                lambda: (
                    sorted(pinned_inputs["months"])
                    if pinned_inputs is not None
                    else discover(client, config.start_month, config.end_month)
                ),
            )
            # Detect upstream revisions rather than silently mixing different historical snapshots.
            remotes = {}
            for month in months:
                remotes[month] = (
                    pinned_inputs["months"][month]
                    if pinned_inputs is not None
                    else remote_metadata(client, trip_url(month))
                )
                if month in previous:
                    check_revision(previous[month], remotes[month])
            completed = {m for m, record in previous.items() if record["status"] == "COMPLETE"}
            selected = select_months(months, completed, config.max_new_months, config.reprocess)
            result["selected_months"] = selected
            LOGGER.info("Selected months: %s", selected)
            if selected:
                zone_path = (
                    Path(pinned_inputs["zones"]["path"])
                    if pinned_inputs is not None
                    else config.landing / "reference" / "taxi_zone_lookup.csv"
                )
                _, zone_bytes = stage(
                    "land_zones",
                    lambda: (
                        verified_landed_file(zone_path, pinned_inputs["zones"])
                        if pinned_inputs is not None
                        else land_file(client, zone_path, remote_metadata(client, ZONE_URL))
                    ),
                )
                result["downloaded_bytes"] += zone_bytes
                zones = (
                    spark.read.schema(
                        "LocationID int, Borough string, Zone string, service_zone string"
                    )
                    .option("header", True)
                    .csv(str(zone_path))
                    .select(
                        F.col("LocationID").alias("location_id"),
                        F.col("Borough").alias("borough"),
                        F.col("Zone").alias("zone"),
                        "service_zone",
                    )
                )
                duplicates = zones.groupBy("location_id").count().filter("count > 1").count()
                if duplicates or zones.filter("location_id IS NULL OR zone IS NULL").count():
                    raise ValueError("Taxi zone lookup contains duplicate or invalid keys.")
                zones.write.format("delta").mode("overwrite").saveAsTable(
                    config.table("silver", "taxi_zones")
                )
                updates = []
                for month in selected:
                    path = config.trip_path(month)
                    landed, downloaded = stage(
                        f"land_{month}",
                        lambda: (
                            verified_landed_file(path, remotes[month])
                            if pinned_inputs is not None
                            else land_file(client, path, remotes[month])
                        ),
                    )
                    result["downloaded_bytes"] += downloaded
                    record = previous.get(month)
                    if record is None or config.reprocess:
                        raw = spark.read.parquet(str(path))
                        bronze = bronze_records(raw, month, str(path), run_id)
                        stage(
                            f"bronze_{month}",
                            lambda: replace_month(bronze, config.table("bronze", "trips"), month),
                        )
                        record = {
                            "source_month": month,
                            **{
                                key: landed[key]
                                for key in (
                                    "url",
                                    "content_length",
                                    "etag",
                                    "last_modified",
                                    "sha256",
                                )
                            },
                            "source_schema_json": raw.schema.json(),
                            "bronze_rows": raw.count(),
                            "valid_rows": None,
                            "quarantine_rows": None,
                            "duplicate_candidate_rows": None,
                            "status": "BRONZE_READY",
                            "updated_at": now(),
                            "run_id": run_id,
                        }
                        update_manifest(spark, config, record)
                    bronze = spark.table(config.table("bronze", "trips")).filter(
                        F.col("source_month") == month
                    )

                    def silver_stage():
                        silver = silver_records(bronze, zones)
                        stats = silver.agg(
                            F.count("*").alias("rows"),
                            F.sum((F.size("quality_errors") == 0).cast("long")).alias("valid"),
                            F.countDistinct("record_fingerprint").alias("distinct_candidates"),
                        ).first()
                        valid = silver.filter(F.size("quality_errors") == 0)
                        quarantine = silver.filter(F.size("quality_errors") > 0)
                        replace_month(valid, config.table("silver", "trips"), month)
                        replace_month(quarantine, config.table("silver", "quarantine"), month)
                        if stats["rows"] != record["bronze_rows"]:
                            raise ValueError(f"Row count changed while transforming {month}.")
                        record.update(
                            valid_rows=stats["valid"] or 0,
                            quarantine_rows=stats["rows"] - (stats["valid"] or 0),
                            duplicate_candidate_rows=stats["rows"] - stats["distinct_candidates"],
                            status="SILVER_READY",
                            updated_at=now(),
                            run_id=run_id,
                        )
                        update_manifest(spark, config, record)

                    stage(f"silver_{month}", silver_stage)
                    result["processed_rows"] += record["bronze_rows"]
                    result["processed_source_bytes"] += record["content_length"]
                    updates.append(record)

                def gold_stage():
                    valid = spark.table(config.table("silver", "trips"))
                    for name, frame in gold_tables(valid).items():
                        frame.write.format("delta").mode("overwrite").saveAsTable(
                            config.table("gold", name)
                        )

                stage("gold", gold_stage)
                result.update(stage("reconcile", lambda: validate_outputs(spark, config)))
                for record in updates:
                    record.update(status="COMPLETE", updated_at=now())
                    update_manifest(spark, config, record)
            result["status"] = "SUCCEEDED" if selected else "NO_NEW_DATA"
    except Exception as error:
        result["status"] = "FAILED"
        result["error_message"] = f"{type(error).__name__}: {error}"[:8000]
        LOGGER.exception("Pipeline failed: %s", run_id)
        raise
    finally:
        result["finished_at"] = now()
        try:
            (
                spark.createDataFrame([result], RUN_SCHEMA)
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(config.table("ops", "pipeline_runs"))
            )
            if stages:
                spark.createDataFrame(stages, STAGE_SCHEMA).write.format("delta").mode(
                    "append"
                ).saveAsTable(config.table("ops", "stage_metrics"))
            report_path = config.landing / "_runs" / f"{run_id}.json"
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps({**result, "stages": stages}, default=str, indent=2))
        except Exception:
            LOGGER.exception("Could not persist operational metrics for run %s", run_id)
            if result["status"] != "FAILED":
                raise
        LOGGER.info("PIPELINE_SUMMARY %s", json.dumps(result, default=str))
    return result
