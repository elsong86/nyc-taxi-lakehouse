import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from nyc_taxi_lakehouse.benchmark import collect_billing, digest
from nyc_taxi_lakehouse.pipeline_experiment import (
    ExperimentConfig,
    assert_equivalent,
    code_hashes,
    execute_phase,
    load_manifest,
    sample_plan,
    summarize_samples,
)
from nyc_taxi_lakehouse.sources import verified_landed_file


def experiment(**values):
    return ExperimentConfig(experiment_id="perf_test", end_month="2024-03", **values)


def test_experiment_isolates_all_outputs_and_reuses_only_pinned_inputs():
    config = experiment(sample_id="standard_1")
    for layer in ("bronze", "silver", "gold", "ops"):
        assert config.table(layer, "trips") == (f"nyc_taxi_dev.perf_test_standard_1_{layer}.trips")
    other = replace(config, sample_id="performance_optimized_1")
    assert other.table("bronze", "trips") != config.table("bronze", "trips")
    assert other.landing != config.landing
    assert other.trip_path("2024-01") == config.trip_path("2024-01")
    for schema, name in (("benchmark", "trips"), ("raw", "landing"), ("ops", "../trips")):
        with pytest.raises(ValueError):
            config.table(schema, name)
    with pytest.raises(ValueError):
        config.trip_path("2024-04")
    with pytest.raises(ValueError):
        experiment().table("bronze", "trips")


@pytest.mark.parametrize(
    "values",
    [
        {"experiment_id": "../production"},
        {"experiment_id": "perf_bad; DROP"},
        {"sample_id": "standard_4"},
        {"sample_id": "../silver"},
        {"reprocess": True},
        {"end_month": "latest"},
        {"max_new_months": 1},
    ],
)
def test_experiment_rejects_unsafe_or_incomparable_configuration(values):
    config = experiment()
    with pytest.raises(ValueError):
        replace(config, **values)


def test_plan_balances_modes_and_repetition_order():
    plan = sample_plan()
    assert len({item["sample_id"] for item in plan}) == 6
    assert [item["performance_target"] for item in plan] == [
        "STANDARD",
        "PERFORMANCE_OPTIMIZED",
        "PERFORMANCE_OPTIMIZED",
        "STANDARD",
        "STANDARD",
        "PERFORMANCE_OPTIMIZED",
    ]


def test_changed_input_or_code_metadata_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(ExperimentConfig, "root", property(lambda self: tmp_path))
    config = experiment()
    inputs = {"months": {"2024-01": {"sha256": "pinned"}}}
    manifest = {
        "experiment_id": config.experiment_id,
        "inputs": inputs,
        "input_sha256": digest(inputs),
        "code_sha256": code_hashes(),
        "plan": sample_plan(),
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    assert load_manifest(config) == manifest
    manifest["inputs"]["months"]["2024-01"]["sha256"] = "changed"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="input metadata changed"):
        load_manifest(config)
    manifest["code_sha256"] = {}
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="source changed"):
        load_manifest(config)


def test_pinned_input_corruption_is_detected_before_processing(tmp_path):
    path = tmp_path / "zones.csv"
    path.write_text("LocationID,Borough,Zone,service_zone\n1,Queens,Test,Test\n")
    record = {"content_length": path.stat().st_size, "sha256": "incorrect"}
    with pytest.raises(ValueError, match="checksum mismatch"):
        verified_landed_file(path, record)


def test_equivalence_compares_row_multiplicity_not_just_counts(spark):
    expected = spark.createDataFrame([(1,), (1,), (2,)], "value int")
    same = spark.createDataFrame([(2,), (1,), (1,)], "value int")
    changed = spark.createDataFrame([(2,), (2,), (1,)], "value int")
    assert_equivalent(expected, same)
    with pytest.raises(ValueError, match="row multiset differs"):
        assert_equivalent(expected, changed)


def test_gold_equivalence_handles_null_keys_and_float_aggregation_noise(spark):
    schema = "month string, trip_count long, average_duration_minutes double"
    expected = spark.createDataFrame([(None, 10, 4.0), ("2024-01", 20, None)], schema)
    close = spark.createDataFrame([(None, 10, 4.0000000001), ("2024-01", 20, None)], schema)
    assert_equivalent(expected, close, keys=["month"])
    for rows in (
        [(None, 11, 4.0), ("2024-01", 20, None)],
        [(None, 10, 4.1), ("2024-01", 20, None)],
        [(None, 10, 4.0), ("2024-01", 20, 0.0)],
        [(None, 10, 4.0)],
    ):
        with pytest.raises(ValueError, match="Gold output differs"):
            assert_equivalent(expected, spark.createDataFrame(rows, schema), keys=["month"])
    with pytest.raises(ValueError, match="duplicate aggregation keys"):
        assert_equivalent(expected, close.union(close), keys=["month"])


def test_summary_does_not_turn_partial_billing_into_a_cost_comparison():
    samples = [
        {
            "job_run_id": str(index),
            "performance_target": "STANDARD",
            "processed_rows": 1000,
            "job_duration_seconds": seconds,
            "pipeline_call_seconds": seconds / 2,
            "timed_processing_seconds": seconds / 4,
            "stages": [{"stage": "bronze_2024-01", "elapsed_seconds": seconds / 8}],
        }
        for index, seconds in enumerate((300, 100, 200))
    ]
    partial = {
        "pipeline_runs": [
            {
                "job_run_id": "0",
                "billing_status": "OBSERVED",
                "list_price_compute_estimate_usd": "0.12",
            }
        ]
    }
    summary = summarize_samples(samples, partial)[0]
    assert summary["samples"] == 3
    assert summary["median_job_duration_seconds"] == 200
    assert summary["median_stage_seconds"]["bronze"] == 25
    assert summary["billing_status"] == "PENDING"
    assert summary["median_list_price_compute_estimate_usd"] is None


def test_billing_uses_explicit_experiment_runs_not_production_ops(monkeypatch):
    calls = []

    def query(cursor, statement, parameters):
        calls.append((statement, parameters))
        return []

    monkeypatch.setattr("nyc_taxi_lakehouse.benchmark.query_rows", query)
    report = collect_billing(
        None,
        "nyc_taxi_dev",
        "isolated-job",
        "warehouse",
        {"started_at": "2026-10-30T00:00:00+00:00", "ended_at": "2026-10-30T01:00:00+00:00"},
        contexts=[{"job_run_id": "sample", "processed_rows": 1000}],
    )
    assert len(calls) == 2
    assert all(".ops.pipeline_runs" not in statement for statement, _ in calls)
    assert calls[0][1]["job_id"] == "isolated-job"
    assert report["pipeline_runs"][0]["billing_status"] == "PENDING"


def test_execution_resume_reuses_recorded_run_and_verifies_actual_mode(tmp_path):
    pytest.importorskip("databricks.sdk")
    requests = []
    run = SimpleNamespace(
        run_id=123,
        state=SimpleNamespace(result_state=SimpleNamespace(value="SUCCESS")),
        effective_performance_target=SimpleNamespace(value="STANDARD"),
        as_dict=lambda: {"run_id": 123},
    )

    def run_now(job_id, **kwargs):
        requests.append(kwargs)
        return SimpleNamespace(run_id=123, result=lambda **kwargs: run)

    client = SimpleNamespace(jobs=SimpleNamespace(run_now=run_now))
    report = {"executions": []}
    for _ in range(2):
        assert (
            execute_phase(
                client,
                experiment(),
                456,
                "measure",
                "STANDARD",
                "standard_1",
                report,
                tmp_path / "report.json",
            )
            is run
        )
    assert len(report["executions"]) == 1
    assert requests[0]["idempotency_token"] == requests[1]["idempotency_token"]
    assert len(requests[0]["idempotency_token"]) == 64
    run.effective_performance_target.value = "PERFORMANCE_OPTIMIZED"
    with pytest.raises(ValueError, match="requested performance mode"):
        execute_phase(
            client,
            experiment(),
            456,
            "measure",
            "STANDARD",
            "standard_1",
            report,
            tmp_path / "report.json",
        )
