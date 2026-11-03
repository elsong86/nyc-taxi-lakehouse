import argparse
import hashlib
import json
import logging
import re
import statistics
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path

from nyc_taxi_lakehouse.benchmark import delta_write_metrics, digest
from nyc_taxi_lakehouse.benchmark_snapshot import SnapshotConfig, storage_details, table_version
from nyc_taxi_lakehouse.config import Config
from nyc_taxi_lakehouse.gold import GOLD_KEYS, write_gold
from nyc_taxi_lakehouse.pipeline_experiment import (
    assert_equivalent,
    audit_versions,
    execute_phase,
    refresh_billing,
    remote_json,
    save_report,
    write_json,
)

LOGGER = logging.getLogger(__name__)
STRATEGIES = ("full", "incremental")
PERFORMANCE_TARGET = "PERFORMANCE_OPTIMIZED"


@dataclass(frozen=True)
class GoldConfig:
    catalog: str = "nyc_taxi_dev"
    experiment_id: str = "unset"
    sample_id: str = "unset"
    job_run_id: str = "local"

    def __post_init__(self):
        Config(catalog=self.catalog)
        if not re.fullmatch(r"gold_[a-z0-9_]{1,48}", self.experiment_id):
            raise ValueError("experiment_id must start with gold_ and use lowercase identifiers.")
        if self.sample_id not in ("seed", "unset") and not re.fullmatch(
            r"(full|incremental)_[1-3]", self.sample_id
        ):
            raise ValueError("Expected full/incremental sample with repetition 1..3.")

    @property
    def root(self) -> Path:
        return Path(
            f"/Volumes/{self.catalog}/benchmark/reports/gold_experiments/{self.experiment_id}"
        )

    @property
    def measurement_path(self) -> Path:
        return self.root / "samples" / f"{self.sample_id}.json"

    @property
    def schema(self) -> str:
        if self.sample_id == "unset":
            raise ValueError("A sample ID is required for an experiment schema.")
        return f"{self.catalog}.{self.experiment_id}_{self.sample_id}"

    def table(self, name: str) -> str:
        if name not in GOLD_KEYS:
            raise ValueError("Only the four experiment Gold tables are allowed.")
        return f"{self.schema}.{name}"


def sample_plan() -> list[dict]:
    return [
        {"sample_id": f"{strategy}_{repetition}", "strategy": strategy}
        for repetition in range(1, 4)
        for strategy in (STRATEGIES if repetition != 2 else tuple(reversed(STRATEGIES)))
    ]


def code_hashes() -> dict:
    package = resources.files("nyc_taxi_lakehouse")
    return {
        name: hashlib.sha256(package.joinpath(name).read_bytes()).hexdigest()
        for name in (
            "gold.py",
            "gold_experiment.py",
            "transforms.py",
            "pipeline_experiment.py",
            "benchmark.py",
            "benchmark_snapshot.py",
            "config.py",
        )
    }


def case(config: GoldConfig, sample_id: str) -> GoldConfig:
    return GoldConfig(config.catalog, config.experiment_id, sample_id, config.job_run_id)


def current_silver(spark, config: GoldConfig, manifest: dict):
    baseline = SnapshotConfig(catalog=config.catalog)
    table = baseline.table("trips")
    return spark.read.option("versionAsOf", manifest["snapshot"]["snapshot_versions"][table]).table(
        table
    )


def prepare(spark, config: GoldConfig) -> dict:
    from pyspark.sql import functions as F

    from nyc_taxi_lakehouse.transforms import dated_trips

    baseline = SnapshotConfig(catalog=config.catalog)
    snapshot = json.loads((baseline.report_root / "dataset_q1_2024.json").read_text())
    audit_versions(spark, snapshot)
    config.root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "experiment_id": config.experiment_id,
        "snapshot": snapshot,
        "code_sha256": code_hashes(),
        "plan": sample_plan(),
        "created_at": datetime.now(UTC).isoformat(),
    }
    valid = current_silver(spark, config, manifest)
    months = {
        row["source_month"]: row["count"] for row in valid.groupBy("source_month").count().collect()
    }
    if sorted(months) != ["2024-01", "2024-02", "2024-03"] or sum(months.values()) != 9551977:
        raise ValueError("The fixed Q1 Silver input does not match the baseline.")
    manifest["silver_rows_by_source_month"] = months
    seed = case(config, "seed")
    spark.sql(f"CREATE SCHEMA {seed.schema}")
    write_gold(spark, valid.filter(F.col("source_month") < "2024-03"), seed.table)
    seed_storage = [storage_details(spark, seed.table(name)) for name in GOLD_KEYS]
    manifest["seed_versions"] = {item["table"]: item["version"] for item in seed_storage}
    manifest["seed_storage"] = seed_storage
    changed = dated_trips(valid.filter(F.col("source_month") == "2024-03"))
    manifest["group_counts"] = {}
    for name, keys in GOLD_KEYS.items():
        rows = changed.filter("airport_trip") if name == "airport_activity" else changed
        manifest["group_counts"][name] = {
            "initial_groups": spark.table(seed.table(name)).count(),
            "q1_groups": spark.table(baseline.table(name)).count(),
            "affected_groups": rows.select(*keys).distinct().count(),
        }
    manifest["initial_versions"] = {}
    for sample in manifest["plan"]:
        target = case(config, sample["sample_id"])
        spark.sql(f"CREATE SCHEMA {target.schema}")
        details = []
        for name in GOLD_KEYS:
            spark.sql(
                f"CREATE TABLE {target.table(name)} DEEP CLONE {seed.table(name)} "
                f"VERSION AS OF {manifest['seed_versions'][seed.table(name)]}"
            )
            details.append(storage_details(spark, target.table(name)))
        manifest["initial_versions"][target.sample_id] = {
            item["table"]: item["version"] for item in details
        }
        if [(item["num_files"], item["active_data_bytes"]) for item in details] != [
            (item["num_files"], item["active_data_bytes"]) for item in seed_storage
        ]:
            raise ValueError("The sample does not have the same seed file layout.")
    manifest["input_sha256"] = digest(
        {
            "snapshot_versions": snapshot["snapshot_versions"],
            "seed_versions": manifest["seed_versions"],
            "source_files": snapshot["source_files"],
            "months": months,
        }
    )
    audit_versions(spark, snapshot)
    write_json(config.root / "manifest.json", manifest)
    return manifest


def load_manifest(config: GoldConfig) -> dict:
    manifest = json.loads((config.root / "manifest.json").read_text())
    if manifest["experiment_id"] != config.experiment_id or manifest["plan"] != sample_plan():
        raise ValueError("The experiment ID or sample plan changed.")
    if manifest["code_sha256"] != code_hashes():
        raise ValueError("Gold experiment source changed.")
    expected = digest(
        {
            "snapshot_versions": manifest["snapshot"]["snapshot_versions"],
            "seed_versions": manifest["seed_versions"],
            "source_files": manifest["snapshot"]["source_files"],
            "months": manifest["silver_rows_by_source_month"],
        }
    )
    if manifest["input_sha256"] != expected:
        raise ValueError("Gold experiment input metadata changed.")
    return manifest


def measure(spark, config: GoldConfig) -> dict:
    from pyspark.sql import functions as F

    manifest = load_manifest(config)
    planned = [row for row in manifest["plan"] if row["sample_id"] == config.sample_id]
    if len(planned) != 1:
        raise ValueError("A measured sample ID is required.")
    for table, version in manifest["initial_versions"][config.sample_id].items():
        if table_version(spark, table) != version:
            raise ValueError("The sample starting state has already been modified.")
    valid = current_silver(spark, config, manifest)
    changed = valid.filter(F.col("source_month") == "2024-03")
    started_at = datetime.now(UTC).isoformat()
    started = time.perf_counter()
    stages = write_gold(
        spark,
        valid,
        config.table,
        changed=changed if planned[0]["strategy"] == "incremental" else None,
    )
    elapsed = time.perf_counter() - started
    tables = []
    for name in GOLD_KEYS:
        history = [
            {
                "version": row["version"],
                "operation": row["operation"],
                "operationMetrics": row["operationMetrics"],
            }
            for row in spark.sql(f"DESCRIBE HISTORY {config.table(name)}").collect()
        ]
        latest = max(history, key=lambda row: row["version"])
        detail = storage_details(spark, config.table(name))
        initial = manifest["initial_versions"][config.sample_id][config.table(name)]
        tables.append(
            {
                **detail,
                "operation": latest["operation"],
                "operation_metrics": latest["operationMetrics"],
                "write_history": [
                    row for row in history if initial < row["version"] <= detail["version"]
                ],
                "write_metrics": delta_write_metrics(history, initial, detail["version"]),
            }
        )
    report = {
        "sample_id": config.sample_id,
        "strategy": planned[0]["strategy"],
        "job_run_id": config.job_run_id,
        "performance_target": PERFORMANCE_TARGET,
        "started_at": started_at,
        "processed_rows": manifest["silver_rows_by_source_month"]["2024-03"],
        "available_silver_rows": sum(manifest["silver_rows_by_source_month"].values()),
        "gold_operation_seconds": elapsed,
        "stages": stages,
        "tables": tables,
        "input_sha256": manifest["input_sha256"],
        "code_sha256": manifest["code_sha256"],
    }
    write_json(config.measurement_path, report)
    return report


def audit(spark, config: GoldConfig) -> dict:
    from nyc_taxi_lakehouse.benchmark_snapshot import assert_frozen_versions

    manifest = load_manifest(config)
    audit_versions(spark, manifest["snapshot"])
    assert_frozen_versions(
        manifest["seed_versions"],
        {table: table_version(spark, table) for table in manifest["seed_versions"]},
    )
    baseline = SnapshotConfig(catalog=config.catalog)
    outcomes = []
    for sample in manifest["plan"]:
        target = case(config, sample["sample_id"])
        measured = json.loads(target.measurement_path.read_text())
        if (
            measured["code_sha256"] != manifest["code_sha256"]
            or measured["input_sha256"] != manifest["input_sha256"]
        ):
            raise ValueError("Sample provenance does not match the experiment.")
        for detail in measured["tables"]:
            if table_version(spark, detail["table"]) != detail["version"]:
                raise ValueError("Measured Gold output was changed after the run.")
        for name, keys in GOLD_KEYS.items():
            assert_equivalent(
                spark.table(baseline.table(name)), spark.table(target.table(name)), keys=keys
            )
        outcomes.append({"sample_id": target.sample_id, "checked_tables": list(GOLD_KEYS)})
    audit_versions(spark, manifest["snapshot"])
    report = {
        "status": "PASSED",
        "samples": outcomes,
        "scope": (
            "All four Gold outputs equal frozen Q1: exact keys, counts and decimal metrics; "
            "floating aggregates allow absolute 1e-9 plus relative 1e-9 tolerance. "
            "Frozen input and seed versions unchanged."
        ),
    }
    write_json(config.root / "audit.json", report)
    return report


def summarize(samples: list[dict], billing: dict | None = None) -> list[dict]:
    costs = {row["job_run_id"]: row for row in (billing or {}).get("pipeline_runs", [])}
    results = []
    for strategy in STRATEGIES:
        selected = [row for row in samples if row["strategy"] == strategy]
        if not selected:
            continue
        summary = {"strategy": strategy, "samples": len(selected)}
        for field in ("gold_operation_seconds", "job_duration_seconds"):
            values = [row[field] for row in selected]
            summary[f"median_{field}"] = statistics.median(values)
            summary[f"min_{field}"] = min(values)
            summary[f"max_{field}"] = max(values)
        summary["median_table_seconds"] = {
            name: statistics.median(
                next(stage["elapsed_seconds"] for stage in row["stages"] if stage["table"] == name)
                for row in selected
            )
            for name in GOLD_KEYS
        }
        summary["median_active_data_bytes"] = statistics.median(
            sum(table["active_data_bytes"] for table in row["tables"]) for row in selected
        )
        summary["median_active_files"] = statistics.median(
            sum(table["num_files"] for table in row["tables"]) for row in selected
        )
        for field in ("data_bytes_added", "data_files_added"):
            totals = [
                sum(table["write_metrics"][field] for table in row["tables"])
                if all(
                    table.get("write_metrics", {}).get(field) is not None for table in row["tables"]
                )
                else None
                for row in selected
            ]
            summary[f"median_{field}"] = (
                statistics.median(totals) if all(total is not None for total in totals) else None
            )
        priced = [costs.get(row["job_run_id"]) for row in selected]
        complete = all(row and row["billing_status"] == "OBSERVED" for row in priced)
        summary["billing_status"] = "OBSERVED_RECORDS" if complete else "PENDING"
        summary["median_job_list_price_compute_estimate_usd"] = (
            statistics.median(float(row["list_price_compute_estimate_usd"]) for row in priced)
            if complete
            else None
        )
        results.append(summary)
    return results


def orchestrate(client, config: GoldConfig, job_id: int, path: Path, warehouse_id: str):
    job = client.jobs.get(job_id).settings
    if (
        job.name != "nyc-taxi-dev-gold-incremental-experiment"
        or job.schedule is not None
        or job.performance_target.value != PERFORMANCE_TARGET
    ):
        raise ValueError("Use the dedicated unscheduled Performance Optimized Gold job.")
    active = list(client.jobs.list_runs(job_id=job_id, active_only=True))
    if path.exists():
        report = json.loads(path.read_text())
        if (report["experiment_id"], report["catalog"], report["job_id"]) != (
            config.experiment_id,
            config.catalog,
            job_id,
        ):
            raise ValueError("Existing report belongs to a different experiment.")
        if any(run.run_id not in {e["run_id"] for e in report["executions"]} for run in active):
            raise ValueError("An unrelated experiment run is active.")
        report.pop("error", None)
    else:
        if active:
            raise ValueError("An experiment run is already active.")
        report = {
            "experiment_id": config.experiment_id,
            "catalog": config.catalog,
            "job_id": job_id,
            "started_at": datetime.now(UTC).isoformat(),
            "executions": [],
            "samples": [],
        }
    report["status"] = "RUNNING"
    try:
        execute_phase(client, config, job_id, "prepare", PERFORMANCE_TARGET, "unset", report, path)
        report["manifest"] = remote_json(client, config.root / "manifest.json")
        save_report(client, config, path, report)
        for sample in sample_plan():
            if any(row["sample_id"] == sample["sample_id"] for row in report["samples"]):
                continue
            run = execute_phase(
                client,
                config,
                job_id,
                "measure",
                PERFORMANCE_TARGET,
                sample["sample_id"],
                report,
                path,
            )
            measurement = remote_json(client, case(config, sample["sample_id"]).measurement_path)
            if measurement["job_run_id"] != str(run.run_id):
                raise ValueError("Measurement belongs to a different job run.")
            measurement["job_duration_seconds"] = (run.end_time - run.start_time) / 1000
            measurement["job_timing"] = {
                name: getattr(run, name)
                for name in (
                    "setup_duration",
                    "execution_duration",
                    "cleanup_duration",
                    "queue_duration",
                )
            }
            report["samples"].append(measurement)
            save_report(client, config, path, report)
            LOGGER.info(
                "Completed %s: %.3fs Gold, %.3fs whole job",
                sample["sample_id"],
                measurement["gold_operation_seconds"],
                measurement["job_duration_seconds"],
            )
        execute_phase(client, config, job_id, "audit", PERFORMANCE_TARGET, "unset", report, path)
        report["audit"] = remote_json(client, config.root / "audit.json")
        report["summary"] = summarize(report["samples"])
        report["status"] = "MEASURED_AND_VERIFIED"
        report["scope"] = (
            "Gold-only March append to identical Jan-Feb Gold starting tables. Both strategies "
            "read pinned Q1 Silver on Performance Optimized compute, with interleaved repetitions "
            "and uncontrolled physical caches. Full rebuild overwrites all aggregates; incremental "
            "recomputes affected aggregation keys from current Silver and MERGEs replacements, "
            "not sums of rounded averages. No Bronze/Silver processing, source downloads, "
            "production rollout, or correction/deletion scenario is included. Gold timer covers "
            "four aggregate writes; job runtime/cost also include startup and metadata reporting. "
            "Preparation and correctness-audit jobs are excluded from measured-run comparisons. "
            "Cost-per-million denominator is newly available March valid Silver rows, not Q1 "
            "input Parquet rows. Missing billing is pending, not zero; three samples are not p95."
        )
        save_report(client, config, path, report)
        refresh_billing(client, config, report, warehouse_id, summarizer=summarize)
        save_report(client, config, path, report)
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        write_json(path, report)
        LOGGER.exception("Gold experiment failed; partial evidence retained at %s", path)
        raise
    return report


def main():
    parser = argparse.ArgumentParser(description="Controlled full vs incremental Gold benchmark.")
    parser.add_argument(
        "--phase", choices=("prepare", "measure", "audit", "orchestrate", "billing"), required=True
    )
    parser.add_argument("--catalog", default="nyc_taxi_dev")
    parser.add_argument("--experiment-id", "--experiment_id")
    parser.add_argument("--sample-id", "--sample_id", default="unset")
    parser.add_argument("--job-run-id", "--job_run_id", default="local")
    parser.add_argument(
        "--performance-target",
        "--performance_target",
        choices=(PERFORMANCE_TARGET,),
        default=PERFORMANCE_TARGET,
    )
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--warehouse-id", default="919966835b70007d")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--auth-type", choices=("databricks-cli", "azure-cli"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    experiment_id = args.experiment_id
    if experiment_id is None:
        if args.phase != "orchestrate":
            parser.error("--experiment-id is required.")
        experiment_id = f"gold_{datetime.now(UTC):%Y%m%d}_{uuid.uuid4().hex[:8]}"
    config = GoldConfig(args.catalog, experiment_id, args.sample_id, args.job_run_id)
    if args.phase in ("orchestrate", "billing"):
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient(auth_type=args.auth_type)
        path = args.report or Path("benchmark-results") / f"{experiment_id}.json"
        if args.phase == "orchestrate":
            if args.job_id is None:
                parser.error("--job-id is required.")
            orchestrate(client, config, args.job_id, path, args.warehouse_id)
        else:
            report = json.loads(path.read_text())
            if (report["experiment_id"], report["catalog"]) != (experiment_id, args.catalog):
                raise ValueError("Report does not belong to this experiment.")
            refresh_billing(client, config, report, args.warehouse_id, summarizer=summarize)
            save_report(client, config, path, report)
    else:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        {"prepare": prepare, "measure": measure, "audit": audit}[args.phase](spark, config)


if __name__ == "__main__":
    main()
