"""Feature engineering with PySpark built-in functions."""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.regions import Region



def _map_column(col: str, mapping: dict[int, str], default: str) -> F.Column:
    pairs = [x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))]
    return F.coalesce(F.create_map(*pairs)[F.col(col)], F.lit(default))


def _bucket(col: F.Column, bins: tuple, labels: tuple) -> F.Column:
    expr = F.when(col < bins[0], labels[0])
    for edge, label in zip(bins[1:], labels[1:-1]):
        expr = expr.when(col < edge, label)
    return expr.otherwise(labels[-1])


def add_features(df: DataFrame, region: Region, zones: Optional[DataFrame] = None) -> DataFrame:
    """Add time, speed, cost, category and zone-name columns (units follow the region profile)."""
    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    hour = F.hour(pickup)
    distance, fare = F.col("trip_distance"), F.col("fare_amount")

    df = (
        df.withColumn("trip_duration_minutes", (F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60.0)
        .withColumn("pickup_date", F.to_date(pickup))
        .withColumn("pickup_year", F.year(pickup))
        .withColumn("pickup_month", F.month(pickup))
        .withColumn("pickup_day", F.dayofmonth(pickup))
        .withColumn("pickup_day_of_week", F.date_format(pickup, "EEEE"))
        .withColumn("pickup_hour", hour)
        .withColumn("is_weekend", F.dayofweek(pickup).isin(1, 7))
        .withColumn("average_speed_mph", distance / (F.col("trip_duration_minutes") / 60.0))
        .withColumn("fare_per_mile", fare / distance)
        .withColumn("tip_percentage", F.when(fare > 0, F.col("tip_amount") / fare * 100.0))
        .withColumn("total_cost_per_mile", F.col("total_amount") / distance)
        .withColumn(
            "time_period",
            F.when(hour.between(5, 11), "Morning").when(hour.between(12, 16), "Afternoon")
            .when(hour.between(17, 21), "Evening").otherwise("Night"),
        )
        .withColumn("trip_distance_category", _bucket(distance, region.distance_bins, region.distance_labels))
        .withColumn("fare_category", _bucket(fare, region.fare_bins, region.fare_labels))
        .withColumn(
            "passenger_category",
            F.when(F.col("passenger_count") == 1, "Solo").when(F.col("passenger_count") == 2, "Pair")
            .otherwise("Group (3+)"),
        )
        .withColumn("vendor_name", _map_column("VendorID", region.vendor_names, "Other"))
        .withColumn("payment_name", _map_column("payment_type", region.payment_names, "Other"))
    )

    if zones is not None:
        pu = F.broadcast(zones.select(F.col("LocationID").alias("PULocationID"),
                                      F.col("Zone").alias("pu_zone")))
        do = F.broadcast(zones.select(F.col("LocationID").alias("DOLocationID"),
                                      F.col("Zone").alias("do_zone")))
        df = df.join(pu, "PULocationID", "left").join(do, "DOLocationID", "left")
    else:
        df = df.withColumn("pu_zone", F.lit(None).cast("string")).withColumn("do_zone", F.lit(None).cast("string"))

    return (
        df.withColumn("pickup_zone", F.coalesce(F.col("pu_zone"), F.concat(F.lit("Zone "), F.col("PULocationID"))))
        .withColumn("dropoff_zone", F.coalesce(F.col("do_zone"), F.concat(F.lit("Zone "), F.col("DOLocationID"))))
        .drop("pu_zone", "do_zone")
    )
