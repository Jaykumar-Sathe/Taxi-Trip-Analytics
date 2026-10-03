"""SparkSession factory with friendly error messages."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _configure_windows_hadoop() -> None:
    """On Windows, Spark needs winutils.exe. Look in %HADOOP_HOME% or C:\\hadoop."""
    if os.name != "nt":
        return
    home = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop"))
    if (home / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(home)
        os.environ["PATH"] = str(home / "bin") + os.pathsep + os.environ.get("PATH", "")
    else:
        print("WARNING: winutils.exe not found in C:\\hadoop\\bin - writing Parquet will fail on Windows.")


def get_spark(app_name: str = "TaxiTripAnalytics", shuffle_partitions: int = 16) -> SparkSession:
    """Create (or reuse) a local SparkSession.

    shuffle_partitions: Spark's default of 200 is wasteful for small data; use a
    small number for the sample and a larger one (e.g. 64-200) for the full data.
    """
    # Make Spark use the same Python as this script (the venv's Python).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    _configure_windows_hadoop()

    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .master(os.getenv("SPARK_MASTER", "local[*]"))
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - environment specific
        raise RuntimeError(
            "Could not start Spark. Make sure Java 11 or 17 is installed "
            "(run `java -version`) and JAVA_HOME is set."
        ) from exc
    spark.sparkContext.setLogLevel("ERROR")
    return spark