import argparse
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from nyc_taxi_lakehouse.config import Config

LOGGER = logging.getLogger(__name__)
DATASET_SCHEMA = (
    "dataset_id string, status string, metadata_json string, "
    "created_at timestamp, completed_at timestamp"
)


@dataclass(frozen=True)
class SnapshotConfig:
    catalog: str = "nyc_taxi_dev"
    dataset_id: str = "q1_2024"
    start_month: str = "2024-01"
    end_month: str = "2024-03"

    def __post_init__(self):
        Config(catalog=self.catalog, start_month=self.start_month, end_month=self.end_month)
        if self.end_month == "latest":
            raise ValueError("A frozen dataset must have a fixed end month.")
        if not re.fullmatch(r"[a-z][a-z0-9_]*", self.dataset_id):
            raise ValueError("dataset_id must be a lowercase SQL identifier.")

    def table(self, name: str) -> str:
        return f"{self.catalog}.benchmark.{self.dataset_id}_{name}"

    @property
    def report_root(self) -> Path:
        return Path(f"/Volumes/{self.catalog}/benchmark/reports")


def bounded_months(frame, config: SnapshotConfig):
    from pyspark.sql import functions as F

    return frame.filter(F.col("source_month").between(config.start_month, config.end_month))


def assert_frozen_versions(expected: dict[str, int], actual: dict[str, int]):
    if expected != actual:
        raise ValueError(f"Frozen table versions changed: expected {expected}, observed {actual}")


def table_version(spark, table: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])


def storage_details(spark, table: str) -> dict:
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(recursive=True)
    return {
        "table": table,
        "version": table_version(spark, table),
        "num_files": detail["numFiles"],
        "active_data_bytes": detail["sizeInBytes"],
        "partition_columns": detail["partitionColumns"],
        "clustering_columns": detail.get("clusteringColumns"),
        "properties": detail["properties"],
    }


def save_dataset(spark, config: SnapshotConfig, metadata: dict, status: str):
    spark.createDataFrame(
        [
            {
                "dataset_id": config.dataset_id,
                "status": status,
                "metadata_json": json.dumps(metadata, default=str, sort_keys=True),
                "created_at": datetime.fromisoformat(metadata["created_at"]),
                "completed_at": datetime.now(UTC) if status == "READY" else None,
            }
        ],
        DATASET_SCHEMA,
    ).createOrReplaceTempView("taxi_dataset_update")
    spark.sql(
        f"MERGE INTO {config.catalog}.benchmark.datasets target "
        "USING taxi_dataset_update source ON target.dataset_id = source.dataset_id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


def freeze(spark, config: SnapshotConfig) -> dict:
    from pyspark.sql import functions as F

    from nyc_taxi_lakehouse.transforms import gold_tables

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {config.catalog}.benchmark.datasets "
        f"({DATASET_SCHEMA}) USING DELTA"
    )
    existing = (
        spark.table(f"{config.catalog}.benchmark.datasets")
        .filter(F.col("dataset_id") == config.dataset_id)
        .collect()
    )
    if len(existing) > 1:
        raise ValueError(f"Multiple dataset reservations exist for {config.dataset_id}.")
    if existing:
        if existing[0]["status"] not in {"BUILDING", "READY"}:
            raise ValueError(f"Unexpected dataset state: {existing[0]['status']}")
        metadata = json.loads(existing[0]["metadata_json"])
        if (
            metadata["start_month"] != config.start_month
            or metadata["end_month"] != config.end_month
        ):
            raise ValueError("Dataset ID already reserved for a different date range.")
        if existing[0]["status"] == "READY":
            observed = {name: table_version(spark, name) for name in metadata["snapshot_versions"]}
            assert_frozen_versions(metadata["snapshot_versions"], observed)
            LOGGER.info("Dataset already frozen; no tables rewritten: %s", config.dataset_id)
            return metadata
    else:
        targets = {
            config.table(name)
            for name in (
                "bronze",
                "trips",
                "quarantine",
                "hourly_zone_demand",
                "monthly_fares",
                "airport_activity",
                "trip_patterns",
            )
        }
        if any(spark.catalog.tableExists(name) for name in targets):
            raise ValueError(
                "Unregistered tables already occupy this dataset ID; refusing to overwrite."
            )
        sources = {
            "bronze": f"{config.catalog}.bronze.trips",
            "trips": f"{config.catalog}.silver.trips",
            "quarantine": f"{config.catalog}.silver.quarantine",
        }
        files = (
            bounded_months(spark.table(f"{config.catalog}.ops.file_manifest"), config)
            .orderBy("source_month")
            .collect()
        )
        if not files or any(row["status"] != "COMPLETE" for row in files):
            raise ValueError("Source months must be completely processed before freezing.")
        from nyc_taxi_lakehouse.sources import published_months

        published_months(
            " ".join(f"yellow_tripdata_{row['source_month']}.parquet" for row in files),
            config.start_month,
            config.end_month,
        )
        metadata = {
            "dataset_id": config.dataset_id,
            "catalog": config.catalog,
            "start_month": config.start_month,
            "end_month": config.end_month,
            "created_at": datetime.now(UTC).isoformat(),
            "source_tables": sources,
            "source_versions": {
                name: table_version(spark, source) for name, source in sources.items()
            },
            "source_files": [
                {key: row[key] for key in ("source_month", "url", "sha256", "content_length")}
                for row in files
            ],
            "expected_counts": {
                "bronze": sum(row["bronze_rows"] for row in files),
                "trips": sum(row["valid_rows"] for row in files),
                "quarantine": sum(row["quarantine_rows"] for row in files),
            },
        }
        save_dataset(spark, config, metadata, "BUILDING")

    for name, source in metadata["source_tables"].items():
        frame = bounded_months(
            spark.read.option("versionAsOf", metadata["source_versions"][name]).table(source),
            config,
        )
        (
            frame.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("source_month")
            .saveAsTable(config.table(name))
        )
        count = spark.table(config.table(name)).count()
        if count != metadata["expected_counts"][name]:
            raise ValueError(f"Snapshot count mismatch for {name}: {count}")

    trips = spark.table(config.table("trips"))
    gold = gold_tables(trips)
    for name, frame in gold.items():
        frame.write.format("delta").mode("overwrite").saveAsTable(config.table(name))
    gold_count = (
        spark.table(config.table("hourly_zone_demand"))
        .agg(F.sum("trip_count").alias("trips"))
        .first()["trips"]
    )
    if gold_count != metadata["expected_counts"]["trips"]:
        raise ValueError("Frozen Gold demand does not reconcile with frozen Silver.")
    names = [config.table(name) for name in metadata["source_tables"]]
    names.extend(config.table(name) for name in gold)
    metadata["storage"] = [storage_details(spark, name) for name in names]
    metadata["snapshot_versions"] = {
        detail["table"]: detail["version"] for detail in metadata["storage"]
    }
    metadata["raw_parquet_bytes"] = sum(row["content_length"] for row in metadata["source_files"])
    metadata["active_delta_data_bytes"] = sum(
        detail["active_data_bytes"] for detail in metadata["storage"]
    )
    metadata["storage_scope"] = (
        "Active Delta data files only; excludes transaction logs, obsolete files, soft deletes, "
        "the live pipeline tables, and Azure transaction charges."
    )
    report_path = config.report_root / f"dataset_{config.dataset_id}.json"
    report_path.write_text(json.dumps(metadata, default=str, indent=2, sort_keys=True))
    save_dataset(spark, config, metadata, "READY")
    LOGGER.info("SNAPSHOT_SUMMARY %s", json.dumps(metadata, default=str))
    return metadata


def main():
    parser = argparse.ArgumentParser(
        description="Materialize an isolated, version-audited snapshot."
    )
    parser.add_argument("--catalog", default="nyc_taxi_dev")
    parser.add_argument("--dataset-id", "--dataset_id", default="q1_2024")
    parser.add_argument("--start-month", "--start_month", default="2024-01")
    parser.add_argument("--end-month", "--end_month", default="2024-03")
    args = parser.parse_args()
    config = SnapshotConfig(args.catalog, args.dataset_id, args.start_month, args.end_month)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from pyspark.sql import SparkSession

    freeze(SparkSession.builder.getOrCreate(), config)


if __name__ == "__main__":
    main()
