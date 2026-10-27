import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        session = (
            SparkSession.builder.master("local[2]")
            .appName("nyc-taxi-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate()
        )
        yield session
        session.stop()
