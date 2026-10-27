import argparse
import logging

from nyc_taxi_lakehouse.config import Config


def parse_config(argv=None) -> Config:
    parser = argparse.ArgumentParser(description="Run the incremental NYC yellow taxi lakehouse.")
    parser.add_argument("--catalog", default="nyc_taxi_dev")
    parser.add_argument("--start-month", "--start_month", default="2024-01")
    parser.add_argument("--end-month", "--end_month", default="latest")
    parser.add_argument("--max-new-months", "--max_new_months", type=int, default=3)
    parser.add_argument("--reprocess", choices=["true", "false"], default="false")
    parser.add_argument("--job-run-id", "--job_run_id", default="local")
    args = parser.parse_args(argv)
    return Config(
        catalog=args.catalog,
        start_month=args.start_month,
        end_month=args.end_month,
        max_new_months=args.max_new_months,
        reprocess=args.reprocess == "true",
        job_run_id=args.job_run_id,
    )


def main():
    config = parse_config()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from pyspark.sql import SparkSession

    from nyc_taxi_lakehouse.pipeline import run

    run(SparkSession.builder.getOrCreate(), config)


if __name__ == "__main__":
    main()
