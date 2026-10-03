"""Aggregations built with the PySpark DataFrame API (groupBy / agg / window)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def _basic_aggs() -> list[F.Column]:
    return [
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
    ]


def build_cube(df: DataFrame) -> DataFrame:
    """Small pre-aggregated 'cube' that powers every filter in the dashboard.

    It is grouped by the filterable dimensions and stores only additive sums,
    so the dashboard can re-aggregate it with Pandas after filtering.
    """
    is_card = F.col("payment_type") == 1
    return df.groupBy(
        "pickup_date", "pickup_hour", "VendorID", "vendor_name", "payment_type", "payment_name",
        "trip_distance_category", "passenger_count",
    ).agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("revenue"),
        F.sum("fare_amount").alias("fare_sum"),
        F.sum("trip_distance").alias("distance_sum"),
        F.sum("trip_duration_minutes").alias("duration_sum"),
        F.sum("tip_amount").alias("tip_sum"),
        # Cash tips are not recorded by TLC, so tip % is computed on card trips only.
        F.sum(F.when(is_card, F.col("fare_amount")).otherwise(0.0)).alias("card_fare_sum"),
        F.sum(F.when(is_card, F.col("tip_amount")).otherwise(0.0)).alias("card_tip_sum"),
    )


def time_metrics(df: DataFrame, *group_cols: str) -> DataFrame:
    return df.groupBy(*group_cols).agg(*_basic_aggs()).orderBy(*group_cols)


def overall_kpis(df: DataFrame) -> DataFrame:
    return df.agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.expr("percentile_approx(trip_distance, 0.5)"), 2).alias("median_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
        F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
        F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
        F.round(F.avg(F.when(F.col("payment_type") == 1, F.col("tip_percentage"))), 2).alias("avg_card_tip_pct"),
    )


def route_metrics(df: DataFrame) -> DataFrame:
    """Per route stats + RANK() window functions (by trips and by revenue)."""
    routes = df.groupBy("PULocationID", "DOLocationID", "pickup_zone", "dropoff_zone").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    ).withColumn("route", F.concat_ws(" → ", "pickup_zone", "dropoff_zone"))
    return (
        routes.withColumn("rank_by_trips", F.rank().over(Window.orderBy(F.desc("trips"))))
        .withColumn("rank_by_revenue", F.rank().over(Window.orderBy(F.desc("revenue"))))
    )


def location_metrics(df: DataFrame) -> DataFrame:
    """Pickup and drop-off zones stacked in one table with a 'role' column."""
    def side(zone_col: str, id_col: str, role: str) -> DataFrame:
        return df.groupBy(F.col(id_col).alias("location_id"), F.col(zone_col).alias("zone")).agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        ).withColumn("role", F.lit(role))

    return side("pickup_zone", "PULocationID", "pickup").unionByName(
        side("dropoff_zone", "DOLocationID", "dropoff"))


def vendor_monthly_ranking(df: DataFrame) -> DataFrame:
    """Monthly vendor revenue ranked inside each month (partitioned window)."""
    monthly = df.groupBy("pickup_year", "pickup_month", "vendor_name").agg(
        F.count("*").alias("trips"), F.round(F.sum("total_amount"), 2).alias("revenue"))
    window = Window.partitionBy("pickup_year", "pickup_month").orderBy(F.desc("revenue"))
    return monthly.withColumn("revenue_rank", F.rank().over(window))


def distance_histogram(df: DataFrame, cap: int = 30) -> DataFrame:
    """Trips per whole distance unit (miles or km depending on region); the last bucket is cap+."""
    return (df.withColumn("distance_bucket", F.least(F.floor("trip_distance"), F.lit(cap)))
            .groupBy("distance_bucket").agg(F.count("*").alias("trips")).orderBy("distance_bucket"))


def duration_histogram(df: DataFrame, width: int = 5, cap: int = 120) -> DataFrame:
    bucket = F.least(F.floor(F.col("trip_duration_minutes") / width) * width, F.lit(cap))
    return df.withColumn("minutes", bucket).groupBy("minutes").agg(F.count("*").alias("trips")).orderBy("minutes")


def scatter_sample(df: DataFrame, total_rows: int, target: int = 5000) -> DataFrame:
    """Small random sample for scatter plots (never ship millions of points to a browser)."""
    fraction = min(1.0, target * 1.2 / max(total_rows, 1))
    return (df.sample(fraction=fraction, seed=42)
            .select("trip_distance", "fare_amount", "total_amount", "tip_amount",
                    "payment_name", "pickup_hour", "trip_duration_minutes")
            .limit(target))


def anomaly_analysis(spark: SparkSession, df: DataFrame, z_threshold: float = 4.0) -> tuple[DataFrame, DataFrame]:
    """Simple rule-based anomaly detection: |z-score| above a threshold.

    Returns (summary, sample_of_most_extreme_trips).
    """
    metrics = ["trip_distance", "fare_amount", "trip_duration_minutes", "fare_per_mile"]
    stats = df.agg(*[F.mean(m).alias(f"{m}_mean") for m in metrics],
                   *[F.stddev(m).alias(f"{m}_std") for m in metrics]).first()
    z = {m: F.abs((F.col(m) - F.lit(stats[f"{m}_mean"])) / F.lit(stats[f"{m}_std"] or 1.0)) for m in metrics}

    flagged = df.select("pickup_zone", "dropoff_zone", "tpep_pickup_datetime", *metrics,
                        *[z[m].alias(f"z_{m}") for m in metrics])
    counts = flagged.agg(*[F.sum((F.col(f"z_{m}") > z_threshold).cast("int")).alias(m) for m in metrics]).first()
    summary = spark.createDataFrame(
        [(m, f"|z| > {z_threshold}", float(stats[f"{m}_mean"]), float(stats[f"{m}_std"] or 0.0), int(counts[m] or 0))
         for m in metrics],
        ["metric", "rule", "mean", "std_dev", "anomalous_trips"],
    )
    sample = (flagged.withColumn("max_z", F.greatest(*[F.col(f"z_{m}") for m in metrics]))
              .filter(F.col("max_z") > z_threshold).orderBy(F.desc("max_z")).limit(500))
    return summary, sample
