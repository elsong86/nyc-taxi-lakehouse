import argparse
import hashlib
import io
import json
import logging
import platform
import re
import statistics
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from nyc_taxi_lakehouse import pipeline_experiment as replay
from nyc_taxi_lakehouse.benchmark import now
from nyc_taxi_lakehouse.config import Config

LOGGER = logging.getLogger(__name__)
KINDS = ("SERVERLESS", "CLASSIC")
NODE_TYPE = "Standard_D4s_v3"
RUNTIME = "17.3.x-scala2.13"


@dataclass(frozen=True)
class ComputeConfig(replay.ExperimentConfig):
    def __post_init__(self):
        Config.__post_init__(self)
        if not re.fullmatch(r"compute_[a-z0-9_]{1,40}", self.experiment_id):
            raise ValueError("Use an isolated compute_ experiment ID.")
        if self.sample_id != "unset" and not re.fullmatch(
            r"(serverless|classic)_[1-3]", self.sample_id
        ):
            raise ValueError("Use a classic/serverless sample with repetition 1..3.")
        if (self.start_month, self.end_month, self.max_new_months, self.reprocess) != (
            "2024-01",
            "2024-03",
            3,
            False,
        ):
            raise ValueError("Compute experiment requires the fixed Q1 replay.")

    @property
    def root(self):
        return Path(
            f"/Volumes/{self.catalog}/benchmark/reports/compute_experiments/{self.experiment_id}"
        )


def sample_plan():
    return [
        {"sample_id": f"{kind.lower()}_{i}", "compute_type": kind}
        for i in range(1, 4)
        for kind in (KINDS if i != 2 else tuple(reversed(KINDS)))
    ]


def code_hashes():
    return {
        **replay.code_hashes(),
        "compute_experiment.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }


def validate_job(settings, kind):
    if (
        settings.name != f"nyc-taxi-dev-compute-{kind.lower()}-experiment"
        or settings.schedule is not None
        or len(settings.tasks or []) != 1
        or settings.max_concurrent_runs != 1
    ):
        raise ValueError("Use only isolated, unscheduled, single-task compute experiment jobs.")
    task = settings.tasks[0]
    if task.max_retries not in (None, 0):
        raise ValueError("Measured tasks must not retry.")
    if kind == "SERVERLESS":
        if (
            task.environment_key != "compute"
            or task.job_cluster_key is not None
            or settings.performance_target.value != "STANDARD"
        ):
            raise ValueError(
                "Serverless comparison requires Standard mode without classic compute."
            )
    else:
        if len(settings.job_clusters or []) != 1 or task.job_cluster_key != "fixed_classic":
            raise ValueError("Classic comparison requires a fresh dedicated job cluster.")
        validate_cluster(settings.job_clusters[0].new_cluster)


def validate_cluster(cluster):
    if (
        cluster.node_type_id != NODE_TYPE
        or cluster.driver_node_type_id != NODE_TYPE
        or cluster.num_workers != 1
        or cluster.autoscale is not None
        or cluster.spark_version not in (RUNTIME, RUNTIME.replace(".x-", ".x-photon-"))
        or cluster.runtime_engine.value != "PHOTON"
        or cluster.data_security_mode.value != "SINGLE_USER"
        or cluster.azure_attributes.availability.value != "ON_DEMAND_AZURE"
        or cluster.instance_pool_id is not None
        or cluster.driver_instance_pool_id is not None
    ):
        raise ValueError("Classic configuration differs from the approved fixed on-demand profile.")


def summary(samples):
    return [
        {
            "compute_type": kind,
            "samples": len(selected),
            **{
                f"median_{key}": statistics.median(row[key] for row in selected)
                for key in (
                    "job_duration_seconds",
                    "pipeline_call_seconds",
                    "timed_processing_seconds",
                )
            },
            "median_setup_seconds": statistics.median(
                row["task_timing"]["setup_duration"] / 1000 for row in selected
            )
            if all(row["task_timing"]["setup_duration"] is not None for row in selected)
            else None,
        }
        for kind in KINDS
        if (selected := [row for row in samples if row["compute_type"] == kind])
    ]


def save(client, config, path, report):
    replay.save_report(client, config, path, report)


def wait_for_termination(client, cluster_id):
    deadline = time.monotonic() + 600
    while True:
        cluster = client.clusters.get(cluster_id)
        if cluster.state.value == "TERMINATED":
            return cluster
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Owned job cluster {cluster_id} did not terminate.")
        time.sleep(10)


def execute(client, config, jobs, phase, kind, sample_id, report, path):
    from databricks.sdk.service.compute import GetEventsOrder
    from databricks.sdk.service.jobs import PerformanceTarget

    job_id = jobs[kind]
    token = hashlib.sha256(f"{config.experiment_id}:{phase}:{sample_id}".encode()).hexdigest()
    options = {
        "idempotency_token": token,
        "job_parameters": {
            "phase": phase,
            "experiment_id": config.experiment_id,
            "sample_id": sample_id,
        },
    }
    if kind == "SERVERLESS":
        options["performance_target"] = PerformanceTarget(
            "STANDARD" if phase == "measure" else "PERFORMANCE_OPTIMIZED"
        )
    pending = client.jobs.run_now(job_id, **options)
    entry = {
        "phase": phase,
        "sample_id": sample_id,
        "compute_type": kind,
        "job_id": job_id,
        "run_id": pending.run_id,
    }
    report["executions"].append(entry)
    replay.write_json(path, report)
    if phase != "prepare":
        save(client, config, path, report)
    LOGGER.info("Started %s %s run %s", phase, sample_id, pending.run_id)
    result = pending.result(timeout=timedelta(hours=1, minutes=10))
    entry["run"] = result.as_dict()
    if result.state.result_state.value != "SUCCESS":
        raise RuntimeError(f"Run {result.run_id} failed: {result.state.state_message}")
    task = result.tasks[0]
    if kind == "SERVERLESS":
        expected = "STANDARD" if phase == "measure" else "PERFORMANCE_OPTIMIZED"
        if result.effective_performance_target.value != expected:
            raise ValueError("Effective serverless performance target changed.")
    else:
        cluster_id = task.cluster_instance.cluster_id
        cluster = client.clusters.get(cluster_id)
        validate_cluster(cluster)
        if cluster.cluster_source.value != "JOB":
            raise ValueError("Classic run did not use a fresh job cluster.")
        prior = [
            row["classic_cluster"]["cluster_id"]
            for row in report["samples"]
            if row["compute_type"] == "CLASSIC"
        ]
        if cluster_id in prior:
            raise ValueError("Classic cluster was reused across measured runs.")
        entry["cluster"] = wait_for_termination(client, cluster_id).as_dict()
        entry["cluster_events"] = [
            event.as_dict()
            for event in client.clusters.events(cluster_id, order=GetEventsOrder.ASC)
        ]
    return result, entry


def cleanup_failed_runs(client, report):
    for entry in report["executions"]:
        run = client.jobs.get_run(entry["run_id"])
        if run.job_id != entry["job_id"]:
            raise ValueError("Cleanup run does not belong to the submitted experiment job.")
        if run.state.life_cycle_state.value not in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            client.jobs.cancel_run(entry["run_id"]).result(timeout=timedelta(minutes=10))
            run = client.jobs.get_run(entry["run_id"])
        if entry["compute_type"] == "CLASSIC":
            for task in run.tasks or []:
                if task.cluster_instance is None or task.cluster_instance.cluster_id is None:
                    continue
                cluster_id = task.cluster_instance.cluster_id
                cluster = client.clusters.get(cluster_id)
                if cluster.cluster_source.value != "JOB":
                    raise ValueError("Refusing to stop compute not created by this job run.")
                if cluster.state.value not in ("TERMINATED", "TERMINATING"):
                    client.clusters.delete(cluster_id).result(timeout=timedelta(minutes=10))
                wait_for_termination(client, cluster_id)
    report["failed_run_cleanup_completed_at"] = now()


def orchestrate(client, config, jobs, path):
    if path.exists():
        raise ValueError("Compute report exists; inspect partial evidence, do not rerun samples.")
    settings = {}
    for kind in KINDS:
        job = client.jobs.get(jobs[kind]).settings
        validate_job(job, kind)
        if list(client.jobs.list_runs(job_id=jobs[kind], active_only=True)):
            raise ValueError("Compute experiment job is already running.")
        settings[kind] = job.as_dict()
    report = {
        "experiment_id": config.experiment_id,
        "catalog": config.catalog,
        "status": "RUNNING",
        "started_at": now(),
        "jobs": jobs,
        "job_settings": settings,
        "executions": [],
        "samples": [],
        "scope": (
            "Three fresh Q1 replays per configuration, interleaved, exact same checksum-pinned "
            "inputs and pipeline source. Classic: one Standard_D4s_v3 driver plus one worker, "
            "Photon, DBR 17.3 LTS, on-demand, no autoscaling/pools/retries; new job cluster each "
            "run. Serverless: Standard performance mode, environment 4; underlying capacity "
            "managed by Databricks. This compares deployment configurations, not identical "
            "hardware/runtime stacks. Setup/total job durations and pipeline-call timers are "
            "recorded separately. Prepare/audit use serverless Performance Optimized outside "
            "measured samples. Caches uncontrolled. Classic DBUs alone are NOT total cost: "
            "Azure VM/disk charges must be collected or separately estimated. No production "
            "or frozen baseline writes."
        ),
    }
    replay.write_json(path, report)
    try:
        execute(client, config, jobs, "prepare", "SERVERLESS", "unset", report, path)
        report["manifest"] = replay.remote_json(client, config.root / "manifest.json")
        archive_root = str(config.root / "measured_sources")
        for name, expected in report["manifest"]["code_sha256"].items():
            payload = (Path(__file__).parent / name).read_bytes()
            if hashlib.sha256(payload).hexdigest() != expected:
                raise ValueError("Deployed pipeline source differs from the controller source.")
            client.files.upload(f"{archive_root}/{name}", io.BytesIO(payload), overwrite=False)
        save(client, config, path, report)
        for sample in sample_plan():
            run, entry = execute(
                client,
                config,
                jobs,
                "measure",
                sample["compute_type"],
                sample["sample_id"],
                report,
                path,
            )
            measurement = replay.remote_json(
                client, config.root / "samples" / sample["sample_id"] / "measurement.json"
            )
            if measurement["job_run_id"] != str(run.run_id):
                raise ValueError("Run ID does not match the recorded measurement.")
            measurement["job_duration_seconds"] = (run.end_time - run.start_time) / 1000
            measurement["task_timing"] = {
                name: getattr(run.tasks[0], name)
                for name in ("setup_duration", "execution_duration", "cleanup_duration")
            }
            measurement["job_timing"] = {
                name: getattr(run, name)
                for name in (
                    "start_time",
                    "end_time",
                    "setup_duration",
                    "execution_duration",
                    "cleanup_duration",
                    "queue_duration",
                    "run_duration",
                )
            }
            if sample["compute_type"] == "CLASSIC":
                measurement["classic_cluster"] = entry["cluster"]
                measurement["cluster_events"] = entry["cluster_events"]
            report["samples"].append(measurement)
            save(client, config, path, report)
            LOGGER.info(
                "Completed %s: %.3fs job, %.3fs pipeline",
                sample["sample_id"],
                measurement["job_duration_seconds"],
                measurement["pipeline_call_seconds"],
            )
        execute(client, config, jobs, "audit", "SERVERLESS", "unset", report, path)
        report["audit"] = replay.remote_json(client, config.root / "audit.json")
        report["summary"] = summary(report["samples"])
        report["status"] = "MEASURED_AND_VERIFIED"
        report["completed_at"] = now()
        report["billing_status"] = "NOT_COLLECTED"
    except Exception as error:
        report["status"] = "FAILED"
        report["error"] = f"{type(error).__name__}: {error}"
        LOGGER.exception("Compute experiment failed; partial evidence retained.")
        try:
            cleanup_failed_runs(client, report)
        except Exception as cleanup_error:
            report["cleanup_error"] = f"{type(cleanup_error).__name__}: {cleanup_error}"
            LOGGER.exception("Owned-run cleanup failed; inspect recorded job/cluster IDs.")
            raise
        raise
    finally:
        replay.write_json(path, report)
        if report.get("manifest"):
            save(client, config, path, report)
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Classic Jobs versus Standard serverless Q1 replay."
    )
    parser.add_argument(
        "--phase", choices=("prepare", "measure", "audit", "orchestrate"), required=True
    )
    parser.add_argument("--experiment-id", "--experiment_id", required=True)
    parser.add_argument("--sample-id", "--sample_id", default="unset")
    parser.add_argument("--compute-type", "--compute_type", choices=KINDS, default="SERVERLESS")
    parser.add_argument("--job-run-id", "--job_run_id", default="local")
    parser.add_argument("--serverless-job-id", type=int)
    parser.add_argument("--classic-job-id", type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = ComputeConfig(
        end_month="2024-03",
        experiment_id=args.experiment_id,
        sample_id=args.sample_id,
        job_run_id=args.job_run_id,
    )
    if args.phase == "orchestrate":
        if args.serverless_job_id is None or args.classic_job_id is None:
            parser.error("Both dedicated job IDs are required.")
        from databricks.sdk import WorkspaceClient

        orchestrate(
            WorkspaceClient(auth_type="azure-cli"),
            config,
            {"SERVERLESS": args.serverless_job_id, "CLASSIC": args.classic_job_id},
            Path("benchmark-results") / f"{args.experiment_id}.json",
        )
        return
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    options = {"plan": sample_plan(), "hashes": code_hashes()}
    if args.phase == "prepare":
        report = replay.prepare(spark, config, **options)
    elif args.phase == "measure":
        report = replay.measure(
            spark, config, args.compute_type, sample_key="compute_type", **options
        )
        report["runtime"] = {
            "spark_version": spark.version,
            "python_version": platform.python_version(),
        }
        replay.write_json(config.landing / "measurement.json", report)
    else:
        report = replay.audit(spark, config, config_type=ComputeConfig, **options)
    LOGGER.info("COMPUTE_EXPERIMENT_RESULT %s", json.dumps(report, default=str))


if __name__ == "__main__":
    main()
