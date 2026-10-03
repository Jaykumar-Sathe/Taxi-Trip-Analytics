"""Cleaning rules and the data-quality report.

Every threshold is a named constant so the rules are easy to read and change.
See docs/methodology.md for the reasoning behind each rule.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.regions import Region

MIN_PICKUP_DATE = "2015-01-01"   # TLC files contain stray rows with dates like 2002 or 2098
MAX_PICKUP_DATE = "2030-01-01"
MIN_DURATION_MIN = 1.0
MAX_PASSENGERS = 6
UNKNOWN_PAYMENT = 5

PICKUP, DROPOFF = "tpep_pickup_datetime", "tpep_dropoff_datetime"
KEY_COLUMNS = [
    "VendorID", PICKUP, DROPOFF, "PULocationID", "DOLocationID",
    "trip_distance", "fare_amount", "total_amount",
]
CRITICAL_COLUMNS = [PICKUP, DROPOFF, "trip_distance", "fare_amount", "total_amount",
                    "PULocationID", "DOLocationID"]


def _duration_minutes() -> F.Column:
    return (F.unix_timestamp(F.col(DROPOFF)) - F.unix_timestamp(F.col(PICKUP))) / 60.0


def clean_trips(df: DataFrame, region: Region) -> DataFrame:
    """Apply the cleaning rules and return the cleaned DataFrame."""
    # Rule 1: rows missing a critical field cannot be analysed -> drop.
    df = df.dropna(subset=CRITICAL_COLUMNS)

    # Rule 2: passenger_count is a driver-entered field; null/0 is common.
    # Impute 1 instead of dropping the trip (revenue is still valid).
    df = df.withColumn(
        "passenger_count",
        F.when(F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0), F.lit(1))
        .otherwise(F.col("passenger_count")),
    )

    # Rule 3: unknown payment type gets its own category instead of being dropped.
    df = df.withColumn("payment_type", F.coalesce(F.col("payment_type"), F.lit(UNKNOWN_PAYMENT)))

    # Rule 4: domain-based validity filters (not statistical outlier removal).
    duration = _duration_minutes()
    df = df.filter(
        F.col(PICKUP).between(F.lit(MIN_PICKUP_DATE).cast("timestamp"), F.lit(MAX_PICKUP_DATE).cast("timestamp"))
        & (duration >= MIN_DURATION_MIN)
        & (duration <= region.max_duration)
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= region.max_distance)
        & (F.col("fare_amount") > 0)
        & (F.col("fare_amount") <= region.max_fare)
        & (F.col("total_amount") > 0)
        & (F.col("total_amount") <= region.max_total)
        & (F.col("passenger_count") <= MAX_PASSENGERS)
        & (F.col("trip_distance") / (duration / 60.0) <= region.max_speed)
    )

    # Rule 5: exact duplicates on the key columns.
    return df.dropDuplicates(KEY_COLUMNS)


def quality_report(spark: SparkSession, raw: DataFrame, cleaned_rows: int, region: Region) -> DataFrame:
    """Data-quality metrics on the RAW data as a small (metric, value) DataFrame."""
    duration = _duration_minutes()
    checks = {
        "total_rows": F.count(F.lit(1)),
        "invalid_distance": F.sum(((F.col("trip_distance") <= 0) | (F.col("trip_distance") > region.max_distance)).cast("int")),
        "invalid_fare": F.sum(((F.col("fare_amount") <= 0) | (F.col("total_amount") <= 0)).cast("int")),
        "invalid_dates": F.sum(((F.col(DROPOFF) <= F.col(PICKUP))
                                | (F.col(PICKUP) < F.lit(MIN_PICKUP_DATE).cast("timestamp"))
                                | (F.col(PICKUP) > F.lit(MAX_PICKUP_DATE).cast("timestamp"))).cast("int")),
        "invalid_duration": F.sum(((duration < MIN_DURATION_MIN) | (duration > region.max_duration)).cast("int")),
        "invalid_passenger_count": F.sum((F.col("passenger_count").isNull()
                                          | (F.col("passenger_count") <= 0)
                                          | (F.col("passenger_count") > MAX_PASSENGERS)).cast("int")),
        "outlier_fare": F.sum((F.col("fare_amount") > region.max_fare).cast("int")),
        "outlier_distance": F.sum((F.col("trip_distance") > region.max_distance).cast("int")),
        "outlier_speed": F.sum(((F.col("trip_distance") / (duration / 60.0)) > region.max_speed).cast("int")),
    }
    for col in ["passenger_count", "trip_distance", "fare_amount", "total_amount",
                "payment_type", PICKUP, DROPOFF]:
        checks[f"missing_{col}"] = F.sum(F.col(col).isNull().cast("int"))

    row = raw.agg(*[expr.alias(name) for name, expr in checks.items()]).first().asDict()
    total = row["total_rows"]
    duplicates = total - raw.dropDuplicates(KEY_COLUMNS).count()

    metrics = [(k, int(v or 0)) for k, v in row.items()]
    metrics.append(("duplicate_rows", int(duplicates)))
    metrics.append(("rows_after_cleaning", int(cleaned_rows)))
    metrics.append(("rows_removed", int(total - cleaned_rows)))
    return spark.createDataFrame(metrics, ["metric", "value"])
