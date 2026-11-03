import argparse
import hashlib
import io
import json
import logging
import re
import shutil
import statistics
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from urllib.parse import urlparse

from nyc_taxi_lakehouse.benchmark import collect_billing, digest, stop_warehouse
from nyc_taxi_lakehouse.benchmark_snapshot import (
    SnapshotConfig,
    assert_frozen_versions,
    table_version,
)
from nyc_taxi_lakehouse.config import Config
from nyc_taxi_lakehouse.gold import GOLD_KEYS
from nyc_taxi_lakehouse.sources import verified_landed_file

LOGGER = logging.getLogger(__name__)
MODES = ("STANDARD", "PERFORMANCE_OPTIMIZED")
LAYERS = ("bronze", "silver", "gold", "ops")


@dataclass(frozen=True)
class ExperimentConfig(Config):
    experiment_id: str = "unset"
    sample_id: str = "unset"

    def __post_init__(self):
        super().__post_init__()
        if not re.fullmatch(r"perf_[a-z0-9_]{1,48}", self.experiment_id):
            raise ValueError("experiment_id must start with perf_ and use lowercase identifiers.")
        if self.sample_id != "unset" and not re.fullmatch(
            r"(standard|performance_optimized)_[1-3]", self.sample_id
        ):
            raise ValueError("sample_id must identify a performance mode and repetition 1..3.")
        if (self.start_month, self.end_month, self.max_new_months, self.reprocess) != (
            "2024-01",
            "2024-03",
            3,
            False,
        ):
            raise ValueError("This experiment requires a fresh January–March 2024 replay.")

    @property
    def root(self) -> Path:
        return Path(
            f"/Volumes/{self.catalog}/benchmark/reports/pipeline_experiments/{self.experiment_id}"
        )

    @property
    def landing(self) -> Path:
        if self.sample_id == "unset":
            raise ValueError("A sample ID is required for measurement output.")
        return self.root / "samples" / self.sample_id

    def trip_path(self, month: str) -> Path:
        if month not in ("2024-01", "2024-02", "2024-03"):
            raise ValueError("Only pinned Q1 source months can be read.")
        return self.root / "inputs" / f"yellow_tripdata_{month}.parquet"

    def table(self, schema: str, name: str) -> str:
        if self.sample_id == "unset" or schema not in LAYERS:
            raise ValueError("A valid sample and pipeline layer are required.")
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Table name must be a lowercase SQL identifier.")
        return f"{self.catalog}.{self.experiment_id}_{self.sample_id}_{schema}.{name}"


def sample_plan() -> list[dict]:
    # Reverse the order in the middle pair to reduce a fixed first/second-position bias.
    return [
        {"sample_id": f"{mode.lower()}_{repetition}", "performance_target": mode}
        for repetition in range(1, 4)
        for mode in (MODES if repetition != 2 else tuple(reversed(MODES)))
    ]


def code_hashes() -> dict:
    package = resources.files("nyc_taxi_lakehouse")
    return {
        name: hashlib.sha256(package.joinpath(name).read_bytes()).hexdigest()
        for name in (
            "config.py",
            "pipeline.py",
            "sources.py",
            "transforms.py",
            "gold.py",
            "pipeline_experiment.py",
        )
    }


def write_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, default=str, indent=2, sort_keys=True))


def audit_versions(spark, snapshot: dict):
    assert_frozen_versions(
        snapshot["snapshot_versions"],
        {table: table_version(spark, table) for table in snapshot["snapshot_versions"]},
    )


def prepare(spark, config: ExperimentConfig) -> dict:
    snapshot_config = SnapshotConfig(catalog=config.catalog)
    snapshot = json.loads((snapshot_config.report_root / "dataset_q1_2024.json").read_text())
    audit_versions(spark, snapshot)
    if snapshot["expected_counts"] != {"bronze": 9554778, "trips": 9551977, "quarantine": 2801}:
        raise ValueError("The registered Q1 baseline counts changed.")
    if sorted(row["source_month"] for row in snapshot["source_files"]) != [
        "2024-01",
        "2024-02",
        "2024-03",
    ]:
        raise ValueError("The baseline must contain exactly the three Q1 source months.")
    config.root.mkdir(parents=True, exist_ok=False)
    inputs = {"months": {}, "zones": None}
    live = Config(catalog=config.catalog)
    for source in snapshot["source_files"]:
        month = source["source_month"]
        path = live.trip_path(month)
        record = json.loads(path.with_suffix(".parquet.manifest.json").read_text())
        if any(record[key] != source[key] for key in ("sha256", "content_length", "url")):
            raise ValueError(f"Raw source differs from the frozen baseline: {month}")
        verified_landed_file(path, record)
        target = config.trip_path(month)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        verified_landed_file(target, record)
        inputs["months"][month] = {**record, "path": str(target)}
    zones = live.landing / "reference" / "taxi_zone_lookup.csv"
    record = json.loads(zones.with_suffix(".csv.manifest.json").read_text())
    verified_landed_file(zones, record)
    target = config.root / "inputs" / zones.name
    shutil.copyfile(zones, target)
    verified_landed_file(target, record)
    inputs["zones"] = {**record, "path": str(target)}
    manifest = {
        "experiment_id": config.experiment_id,
        "created_at": datetime.now(UTC).isoformat(),
        "snapshot": snapshot,
        "inputs": inputs,
        "input_sha256": digest(inputs),
        "code_sha256": code_hashes(),
        "plan": sample_plan(),
        "scope": (
            "Fresh Bronze/Silver/Gold replay from checksum-pinned Azure landing files. "
            "No live source discovery or internet downloads. New schemas for each sample; "
            "no previous output, no task retries. Physical caches are not forcibly cleared; "
            "run order is interleaved. Not directly comparable to the original ingestion run."
        ),
    }
    write_json(config.root / "manifest.json", manifest)
    return manifest


def load_manifest(config: ExperimentConfig) -> dict:
    manifest = json.loads((config.root / "manifest.json").read_text())
    if manifest["experiment_id"] != config.experiment_id:
        raise ValueError("Experiment manifest ID mismatch.")
    if manifest["code_sha256"] != code_hashes():
        raise ValueError("Pipeline source changed during the experiment.")
    if manifest["input_sha256"] != digest(manifest["inputs"]):
        raise ValueError("Pinned input metadata changed during the experiment.")
    if manifest["plan"] != sample_plan():
        raise ValueError("Experiment sample plan changed.")
    return manifest


def measure(spark, config: ExperimentConfig, performance_target: str) -> dict:
    from nyc_taxi_lakehouse.pipeline import run

    manifest = load_manifest(config)
    if {"sample_id": config.sample_id, "performance_target": performance_target} not in manifest[
        "plan"
    ]:
        raise ValueError("Sample and performance mode do not match the experiment plan.")
    # CREATE without IF NOT EXISTS refuses dirty/reused starting states.
    for layer in LAYERS:
        schema = config.table(layer, "trips").rsplit(".", 1)[0]
        spark.sql(f"CREATE SCHEMA {schema}")
    started = time.perf_counter()
    result = run(spark, config, pinned_inputs=manifest["inputs"])
    elapsed = time.perf_counter() - started
    if result["status"] != "SUCCEEDED" or result["selected_months"] != sorted(
        manifest["inputs"]["months"]
    ):
        raise ValueError("The sample did not process all three pinned months successfully.")
    expected = manifest["snapshot"]["expected_counts"]
    if (result["bronze_rows"], result["valid_rows"], result["quarantine_rows"]) != (
        expected["bronze"],
        expected["trips"],
        expected["quarantine"],
    ):
        raise ValueError("Sample row counts differ from the frozen Q1 baseline.")
    if result["downloaded_bytes"] != 0:
        raise ValueError("A pinned replay must not download new source data.")
    operational = json.loads((config.landing / "_runs" / f"{result['run_id']}.json").read_text())
    report = {
        **result,
        "sample_id": config.sample_id,
        "performance_target": performance_target,
        "pipeline_call_seconds": elapsed,
        "timed_processing_seconds": (result["finished_at"] - result["started_at"]).total_seconds(),
        "stages": operational["stages"],
        "input_sha256": manifest["input_sha256"],
        "code_sha256": manifest["code_sha256"],
    }
    write_json(config.landing / "measurement.json", report)
    return report


def assert_equivalent(expected, actual, *, keys: list[str] | None = None):
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, FloatType

    if expected.schema != actual.select(expected.columns).schema:
        raise ValueError("Output schema differs from the frozen baseline.")
    actual = actual.select(expected.columns)
    if keys is None:
        if (
            expected.exceptAll(actual).limit(1).count()
            or actual.exceptAll(expected).limit(1).count()
        ):
            raise ValueError("Output row multiset differs from the frozen baseline.")
        return
    left = expected.withColumn("_present", F.lit(True)).alias("expected")
    right = actual.withColumn("_present", F.lit(True)).alias("actual")
    condition = F.lit(True)
    for key in keys:
        condition = condition & F.col(f"expected.{key}").eqNullSafe(F.col(f"actual.{key}"))
    joined = left.join(right, condition, "full")
    differs = F.col("expected._present").isNull() | F.col("actual._present").isNull()
    for field in expected.schema.fields:
        if field.name in keys:
            continue
        a, b = F.col(f"expected.{field.name}"), F.col(f"actual.{field.name}")
        equal = a.eqNullSafe(b)
        if isinstance(field.dataType, (DoubleType, FloatType)):
            equal = equal | F.coalesce(
                F.abs(a - b) <= F.lit(1e-9) + F.lit(1e-9) * F.abs(a), F.lit(False)
            )
        differs = differs | ~equal
    if joined.filter(differs).limit(1).count():
        raise ValueError("Gold output differs beyond floating-point aggregation tolerance.")
    for frame in (expected, actual):
        if frame.groupBy(*keys).count().filter("count > 1").limit(1).count():
            raise ValueError("Gold output contains duplicate aggregation keys.")


def audit(spark, config: ExperimentConfig) -> dict:
    manifest = load_manifest(config)
    snapshot = manifest["snapshot"]
    audit_versions(spark, snapshot)
    baseline = SnapshotConfig(catalog=config.catalog)
    outcomes = []
    for sample in manifest["plan"]:
        case = ExperimentConfig(
            catalog=config.catalog,
            end_month="2024-03",
            experiment_id=config.experiment_id,
            sample_id=sample["sample_id"],
        )
        measurement = json.loads((case.landing / "measurement.json").read_text())
        if (
            measurement["input_sha256"] != manifest["input_sha256"]
            or measurement["code_sha256"] != manifest["code_sha256"]
        ):
            raise ValueError("Measurement provenance does not match the experiment.")
        checked = []
        for frozen, layer, name in (
            ("bronze", "bronze", "trips"),
            ("trips", "silver", "trips"),
            ("quarantine", "silver", "quarantine"),
        ):
            ignored = ("source_file", "ingestion_run_id", "ingested_at", "processed_at")
            assert_equivalent(
                spark.table(baseline.table(frozen)).drop(*ignored),
                spark.table(case.table(layer, name)).drop(*ignored),
            )
            checked.append(f"{layer}.{name}")
        for name, keys in GOLD_KEYS.items():
            assert_equivalent(
                spark.table(baseline.table(name)),
                spark.table(case.table("gold", name)),
                keys=keys,
            )
            checked.append(f"gold.{name}")
        outcomes.append({"sample_id": sample["sample_id"], "checked_tables": checked})
    audit_versions(spark, snapshot)
    report = {
        "status": "PASSED",
        "samples": outcomes,
        "scope": (
            "Exact Bronze/Silver/quarantine row multisets excluding per-run provenance. "
            "Gold keys and exact metrics match; floating aggregates use absolute 1e-9 plus "
            "relative 1e-9 tolerance. Frozen table versions unchanged."
        ),
    }
    write_json(config.root / "audit.json", report)
    return report


def summarize_samples(samples: list[dict], billing: dict | None = None) -> list[dict]:
    costs = {row["job_run_id"]: row for row in (billing or {}).get("pipeline_runs", [])}
    summaries = []
    for mode in MODES:
        selected = [sample for sample in samples if sample["performance_target"] == mode]
        if not selected:
            continue
        summary = {"performance_target": mode, "samples": len(selected)}
        for field in ("job_duration_seconds", "pipeline_call_seconds", "timed_processing_seconds"):
            values = [sample[field] for sample in selected]
            summary[f"median_{field}"] = statistics.median(values)
            summary[f"min_{field}"] = min(values)
            summary[f"max_{field}"] = max(values)
        summary["median_stage_seconds"] = {
            stage: statistics.median(
                sum(
                    item["elapsed_seconds"]
                    for item in sample["stages"]
                    if item["stage"] == stage or item["stage"].startswith(f"{stage}_")
                )
                for sample in selected
            )
            for stage in ("bronze", "silver", "gold", "reconcile")
        }
        summary["median_full_job_rows_per_second"] = statistics.median(
            sample["processed_rows"] / sample["job_duration_seconds"] for sample in selected
        )
        priced = [costs.get(sample["job_run_id"]) for sample in selected]
        complete = all(row and row["billing_status"] == "OBSERVED" for row in priced)
        summary["billing_status"] = "OBSERVED_RECORDS" if complete else "PENDING"
        summary["median_list_price_compute_estimate_usd"] = (
            statistics.median(float(row["list_price_compute_estimate_usd"]) for row in priced)
            if complete
            else None
        )
        summary["median_list_price_usd_per_million_processed_rows"] = (
            statistics.median(
                float(row["list_price_usd_per_million_processed_rows"]) for row in priced
            )
            if complete
            else None
        )
        summaries.append(summary)
    return summaries


def remote_json(client, path: Path) -> dict:
    return json.loads(client.files.download(str(path)).contents.read())


def save_report(client, config: ExperimentConfig, path: Path, report: dict):
    report["updated_at"] = datetime.now(UTC).isoformat()
    write_json(path, report)
    client.files.upload(
        str(config.root / "experiment_report.json"), io.BytesIO(path.read_bytes()), overwrite=True
    )


def execute_phase(client, config, job_id, phase, mode, sample_id, report, path):
    from databricks.sdk.service.jobs import PerformanceTarget

    token = hashlib.sha256(f"{config.experiment_id}:{phase}:{sample_id}".encode()).hexdigest()
    pending = client.jobs.run_now(
        job_id,
        performance_target=PerformanceTarget(mode),
        idempotency_token=token,
        job_parameters={
            "phase": phase,
            "experiment_id": config.experiment_id,
            "sample_id": sample_id,
            "performance_target": mode,
        },
    )
    entries = [
        entry
        for entry in report["executions"]
        if entry["phase"] == phase and entry["sample_id"] == sample_id
    ]
    if entries:
        entry = entries[0]
        if entry["run_id"] != pending.run_id or entry["performance_target"] != mode:
            raise ValueError("Idempotent execution no longer matches the recorded run.")
    else:
        entry = {
            "phase": phase,
            "sample_id": sample_id,
            "performance_target": mode,
            "run_id": pending.run_id,
        }
        report["executions"].append(entry)
    # The prepare phase creates the remote experiment directory; persist locally first.
    write_json(path, report)
    LOGGER.info("Started %s %s run %s", phase, sample_id, pending.run_id)
    result = pending.result(timeout=timedelta(hours=2))
    entry["run"] = result.as_dict()
    if result.state.result_state.value != "SUCCESS":
        raise RuntimeError(f"Experiment run failed: {result.run_id}: {result.state.state_message}")
    if (
        result.effective_performance_target is None
        or result.effective_performance_target.value != mode
    ):
        raise ValueError(f"Run {result.run_id} did not use the requested performance mode.")
    return result


def refresh_billing(client, config, report, warehouse_id, *, summarizer=summarize_samples):
    from databricks import sql

    warehouse = client.warehouses.get(warehouse_id)
    if (
        warehouse.name != "nyc-taxi-dev-benchmark"
        or warehouse.state.value != "STOPPED"
        or warehouse.num_active_sessions
    ):
        raise ValueError("Billing collection requires the stopped, dedicated benchmark warehouse.")
    window = {"started_at": report["started_at"], "ended_at": datetime.now(UTC).isoformat()}
    contexts = [
        {
            key: sample[key]
            for key in (
                "job_run_id",
                "sample_id",
                "performance_target",
                "processed_rows",
                "started_at",
            )
        }
        for sample in report["samples"]
    ]
    try:
        client.warehouses.start(warehouse_id).result(timeout=timedelta(minutes=10))
        with (
            sql.connect(
                server_hostname=urlparse(client.config.host).hostname,
                http_path=warehouse.odbc_params.path,
                credentials_provider=lambda: client.config.authenticate,
                user_agent_entry="nyc-taxi-pipeline-experiment",
            ) as connection,
            connection.cursor() as cursor,
        ):
            report["billing"] = collect_billing(
                cursor,
                config.catalog,
                str(report["job_id"]),
                warehouse_id,
                window,
                contexts=contexts,
            )
    finally:
        stop_warehouse(client, warehouse_id)
    report["summary"] = summarizer(report["samples"], report["billing"])


def orchestrate(client, config, job_id, path, warehouse_id):
    job = client.jobs.get(job_id).settings
    if job.name != "nyc-taxi-dev-pipeline-performance-experiment" or job.schedule is not None:
        raise ValueError("Use only the dedicated, unscheduled pipeline experiment job.")
    active = list(client.jobs.list_runs(job_id=job_id, active_only=True))
    if path.exists():
        report = json.loads(path.read_text())
        if (
            report["experiment_id"] != config.experiment_id
            or report["catalog"] != config.catalog
            or report["job_id"] != job_id
        ):
            raise ValueError("Existing report belongs to a different experiment.")
        known = {entry["run_id"] for entry in report["executions"]}
        if any(run.run_id not in known for run in active):
            raise ValueError("An unrelated experiment run is active.")
        report.pop("error", None)
        report["status"] = "RUNNING"
    else:
        if active:
            raise ValueError("The experiment job already has an active run.")
        report = {
            "experiment_id": config.experiment_id,
            "catalog": config.catalog,
            "job_id": job_id,
            "started_at": datetime.now(UTC).isoformat(),
            "status": "RUNNING",
            "executions": [],
            "samples": [],
        }
    try:
        execute_phase(
            client, config, job_id, "prepare", "PERFORMANCE_OPTIMIZED", "unset", report, path
        )
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
                sample["performance_target"],
                sample["sample_id"],
                report,
                path,
            )
            measurement = remote_json(
                client, config.root / "samples" / sample["sample_id"] / "measurement.json"
            )
            if measurement["job_run_id"] != str(run.run_id):
                raise ValueError("Measurement job run ID does not match the submitted run.")
            measurement["job_duration_seconds"] = (run.end_time - run.start_time) / 1000
            measurement["job_timing"] = {
                name: getattr(run, name)
                for name in (
                    "start_time",
                    "end_time",
                    "run_duration",
                    "setup_duration",
                    "execution_duration",
                    "cleanup_duration",
                    "queue_duration",
                )
            }
            report["samples"].append(measurement)
            save_report(client, config, path, report)
            LOGGER.info(
                "Completed %s: %.2fs wall clock, %.2fs pipeline call",
                sample["sample_id"],
                measurement["job_duration_seconds"],
                measurement["pipeline_call_seconds"],
            )
        execute_phase(
            client, config, job_id, "audit", "PERFORMANCE_OPTIMIZED", "unset", report, path
        )
        report["audit"] = remote_json(client, config.root / "audit.json")
        report["summary"] = summarize_samples(report["samples"])
        report["status"] = "MEASURED_AND_VERIFIED"
        save_report(client, config, path, report)
        refresh_billing(client, config, report, warehouse_id)
        report["scope"] = (
            "Three measured fresh-table Q1 replays per mode, interleaved, with identical code "
            "and pinned inputs. Job wall clock includes compute startup, schema setup, input "
            "verification and metrics persistence. Pipeline-call timer includes initialization "
            "and report writes; internal processing timer excludes those operations. "
            "Preparation and equivalence-audit runs are excluded from measured runtime/cost. "
            "Physical cache state is uncontrolled; no p95 claims from three repetitions. "
            "Costs are observed list-price estimates and may receive later corrections."
        )
        save_report(client, config, path, report)
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        write_json(path, report)
        LOGGER.exception("Experiment failed; partial evidence retained at %s", path)
        raise
    return report


def main():
    parser = argparse.ArgumentParser(description="Controlled serverless pipeline mode comparison.")
    parser.add_argument(
        "--phase", choices=("prepare", "measure", "audit", "orchestrate", "billing"), required=True
    )
    parser.add_argument("--catalog", default="nyc_taxi_dev")
    parser.add_argument("--experiment-id", "--experiment_id", default=None)
    parser.add_argument("--sample-id", "--sample_id", default="unset")
    parser.add_argument(
        "--performance-target", "--performance_target", choices=MODES, default=MODES[0]
    )
    parser.add_argument("--job-run-id", "--job_run_id", default="local")
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--warehouse-id", default="919966835b70007d")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    experiment_id = args.experiment_id
    if experiment_id is None:
        if args.phase != "orchestrate":
            parser.error("--experiment-id is required for this phase.")
        experiment_id = f"perf_{datetime.now(UTC):%Y%m%d}_{uuid.uuid4().hex[:8]}"
    config = ExperimentConfig(
        catalog=args.catalog,
        end_month="2024-03",
        experiment_id=experiment_id,
        sample_id=args.sample_id,
        job_run_id=args.job_run_id,
    )
    if args.phase in ("orchestrate", "billing"):
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient(auth_type="databricks-cli")
        path = args.report or Path("benchmark-results") / f"{experiment_id}.json"
        if args.phase == "orchestrate":
            if args.job_id is None:
                parser.error("--job-id is required for orchestration.")
            orchestrate(client, config, args.job_id, path, args.warehouse_id)
        else:
            report = json.loads(path.read_text())
            if report["experiment_id"] != experiment_id or report["catalog"] != args.catalog:
                raise ValueError("Report does not belong to this experiment/catalog.")
            refresh_billing(client, config, report, args.warehouse_id)
            save_report(client, config, path, report)
    else:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        if args.phase == "prepare":
            prepare(spark, config)
        elif args.phase == "measure":
            measure(spark, config, args.performance_target)
        else:
            audit(spark, config)


if __name__ == "__main__":
    main()
