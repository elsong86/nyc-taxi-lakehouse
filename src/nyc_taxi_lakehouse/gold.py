import time

GOLD_KEYS = {
    "hourly_zone_demand": [
        "pickup_date",
        "pickup_hour",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
    ],
    "monthly_fares": ["pickup_month"],
    "airport_activity": ["pickup_month", "pickup_location_id", "dropoff_location_id"],
    "trip_patterns": ["pickup_month", "pickup_hour"],
}


def affected_rows(current, changed, keys: list[str]):
    from pyspark.sql import functions as F

    left = current.alias("current")
    right = changed.select(*keys).distinct().alias("changed")
    condition = F.lit(True)
    for key in keys:
        condition = condition & F.col(f"current.{key}").eqNullSafe(F.col(f"changed.{key}"))
    return left.join(right, condition, "left_semi")


def write_gold(spark, valid, table, *, changed=None) -> list[dict]:
    from nyc_taxi_lakehouse.transforms import gold_tables

    stages = []
    for name, frame in gold_tables(valid, changed=changed).items():
        started = time.perf_counter()
        if changed is None:
            frame.write.format("delta").mode("overwrite").saveAsTable(table(name))
        else:
            view = f"taxi_gold_updates_{name}"
            frame.createOrReplaceTempView(view)
            condition = " AND ".join(f"target.{key} <=> source.{key}" for key in GOLD_KEYS[name])
            spark.sql(
                f"MERGE INTO {table(name)} target USING {view} source ON {condition} "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            ).collect()
        stages.append({"table": name, "elapsed_seconds": time.perf_counter() - started})
    return stages
