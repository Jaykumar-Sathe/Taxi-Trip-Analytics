"""Load the .sql files and run every named query against the `taxi_trips` view."""
from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.config import SQL_DIR

VIEW_NAME = "taxi_trips"
_NAME_PATTERN = re.compile(r"^--\s*name:\s*(\w+)\s*$", flags=re.MULTILINE)


def load_queries(sql_dir: Path = SQL_DIR) -> dict[str, str]:
    """Parse `-- name: query_name` blocks from every .sql file."""
    queries: dict[str, str] = {}
    files = sorted(sql_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No .sql files found in {sql_dir}")
    for file in files:
        parts = _NAME_PATTERN.split(file.read_text(encoding="utf-8"))
        for name, body in zip(parts[1::2], parts[2::2]):
            queries[name] = body.strip().rstrip(";")
    return queries


def run_queries(spark: SparkSession, df: DataFrame, show: bool = True) -> dict[str, DataFrame]:
    """Register the temp view and execute all queries (lazy DataFrames are returned)."""
    df.createOrReplaceTempView(VIEW_NAME)
    results: dict[str, DataFrame] = {}
    for name, sql in load_queries().items():
        result = spark.sql(sql)
        results[name] = result
        if show:
            print(f"\n=== SQL: {name} ===")
            result.show(10, truncate=False)
    return results
