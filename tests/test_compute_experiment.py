from dataclasses import replace
from types import SimpleNamespace

import pytest

from nyc_taxi_lakehouse.compute_experiment import (
    ComputeConfig,
    sample_plan,
    summary,
    validate_cluster,
    validate_job,
)


def test_compute_outputs_are_isolated_and_inputs_shared_between_modes():
    a = ComputeConfig(experiment_id="compute_test", end_month="2024-03", sample_id="classic_1")
    b = replace(a, sample_id="serverless_1")
    assert a.table("silver", "trips") != b.table("silver", "trips")
    assert a.landing != b.landing
    assert a.trip_path("2024-01") == b.trip_path("2024-01")
    assert "/compute_experiments/compute_test/" in str(a.landing)
    for values in (
        {"experiment_id": "perf_production"},
        {"sample_id": "classic_4"},
        {"sample_id": "../gold"},
        {"reprocess": True},
        {"end_month": "latest"},
    ):
        with pytest.raises(ValueError):
            replace(a, **values)


def test_compute_plan_has_three_samples_per_kind_and_reverses_middle_pair():
    assert [row["compute_type"] for row in sample_plan()] == [
        "SERVERLESS",
        "CLASSIC",
        "CLASSIC",
        "SERVERLESS",
        "SERVERLESS",
        "CLASSIC",
    ]
    assert len({row["sample_id"] for row in sample_plan()}) == 6


def test_approved_classic_configuration_rejects_spot_scaling_and_hardware_changes():
    cluster = SimpleNamespace(
        node_type_id="Standard_D4s_v3",
        driver_node_type_id="Standard_D4s_v3",
        num_workers=1,
        autoscale=None,
        spark_version="17.3.x-scala2.13",
        runtime_engine=SimpleNamespace(value="PHOTON"),
        data_security_mode=SimpleNamespace(value="SINGLE_USER"),
        azure_attributes=SimpleNamespace(availability=SimpleNamespace(value="ON_DEMAND_AZURE")),
        instance_pool_id=None,
        driver_instance_pool_id=None,
    )
    validate_cluster(cluster)
    for key, value in (
        ("num_workers", 2),
        ("autoscale", object()),
        ("node_type_id", "Standard_D8s_v3"),
        ("instance_pool_id", "warm-pool"),
        ("azure_attributes", SimpleNamespace(availability=SimpleNamespace(value="SPOT_AZURE"))),
    ):
        invalid = SimpleNamespace(**{**vars(cluster), key: value})
        with pytest.raises(ValueError, match="approved"):
            validate_cluster(invalid)


def test_summary_preserves_missing_setup_and_does_not_invent_full_costs():
    rows = [
        {
            "compute_type": "CLASSIC",
            "job_duration_seconds": value,
            "pipeline_call_seconds": value / 2,
            "timed_processing_seconds": value / 3,
            "task_timing": {"setup_duration": None},
        }
        for value in (300, 100, 200)
    ]
    observed = summary(rows)[0]
    assert observed["median_job_duration_seconds"] == 200
    assert observed["median_setup_seconds"] is None
    assert not any("cost" in key for key in observed)


def test_job_guard_accepts_api_omitted_zero_retry_default_but_rejects_retries():
    task = SimpleNamespace(max_retries=None, environment_key="compute", job_cluster_key=None)
    settings = SimpleNamespace(
        name="nyc-taxi-dev-compute-serverless-experiment",
        schedule=None,
        tasks=[task],
        max_concurrent_runs=1,
        performance_target=SimpleNamespace(value="STANDARD"),
    )
    validate_job(settings, "SERVERLESS")
    task.max_retries = 1
    with pytest.raises(ValueError, match="must not retry"):
        validate_job(settings, "SERVERLESS")
