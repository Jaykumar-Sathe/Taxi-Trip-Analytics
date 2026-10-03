"""Data ingestion: read Parquet files with an explicit, enforced schema."""
from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# Column -> Spark type. NYC TLC changes types between months (e.g. int vs double
# for passenger_count), so every file is cast to this schema before the union.
TRIP_SCHEMA: dict[str, str] = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
    "fare_amount", "total_amount", "PULocationID", "DOLocationID",
]


def _conform(df: DataFrame, source: str) -> DataFrame:
    """Select + cast columns to TRIP_SCHEMA (case-insensitive); missing optional cols become null."""
    lookup = {c.lower(): c for c in df.columns}
    missing = [c for c in REQUIRED_COLUMNS if c.lower() not in lookup]
    if missing:
        raise ValueError(f"{source} is missing required columns: {missing}")
    cols = []
    for name, dtype in TRIP_SCHEMA.items():
        src = lookup.get(name.lower())
        expr = F.col(src) if src else F.lit(None)
        cols.append(expr.cast(dtype).alias(name))
    return df.select(*cols)


def read_trips(spark: SparkSession, path: Path) -> DataFrame:
    """Read one Parquet file or every *.parquet file inside a folder."""
    path = Path(path)
    if path.is_dir():
        files = sorted(path.glob("*.parquet"))
    elif path.is_file():
        files = [path]
    else:
        raise FileNotFoundError(f"Data path does not exist: {path}")
    if not files:
        raise FileNotFoundError(
            f"No .parquet files found in {path}. Run `python scripts/download_data.py` first."
        )
    frames = [_conform(spark.read.parquet(str(f)), f.name) for f in files]
    return reduce(lambda a, b: a.unionByName(b), frames)


def load_zone_lookup(spark: SparkSession, candidates: tuple) -> Optional[DataFrame]:
    """Taxi zone lookup (LocationID -> Borough, Zone). Returns None if not available."""
    for candidate in candidates:
        if candidate.exists():
            return (
                spark.read.csv(str(candidate), header=True)
                .select(
                    F.col("LocationID").cast("int").alias("LocationID"),
                    F.col("Borough"),
                    F.col("Zone"),
                )
            )
    return None


def describe_dataframe(df: DataFrame, sample_rows: int = 5) -> int:
    """Print schema, sample rows and row count. Returns the row count."""
    df.printSchema()
    df.show(sample_rows, truncate=False)
    rows = df.count()
    print(f"Row count: {rows:,}")
    return rows
