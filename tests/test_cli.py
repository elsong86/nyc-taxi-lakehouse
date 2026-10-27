from nyc_taxi_lakehouse.cli import parse_config


def test_databricks_job_parameters_map_to_pipeline_config():
    config = parse_config(
        [
            "--start_month",
            "2024-01",
            "--end_month",
            "2024-03",
            "--max_new_months",
            "1",
            "--reprocess",
            "true",
            "--job_run_id",
            "123",
        ]
    )
    assert config.start_month == "2024-01"
    assert config.end_month == "2024-03"
    assert config.max_new_months == 1
    assert config.reprocess is True
    assert config.job_run_id == "123"
