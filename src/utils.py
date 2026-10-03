"""Small helpers shared by the pipeline scripts."""
from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from pyspark.sql import DataFrame


@contextmanager
def timed(label: str) -> Iterator[None]:
    start = time.time()
    print(f"-> {label} ...")
    yield
    print(f"   done in {time.time() - start:.1f}s")


def save_parquet(df: DataFrame, path: Path, coalesce: int | None = 1,
                 partition_by: list[str] | None = None) -> None:
    """Write a DataFrame as Parquet (overwrite).

    Small aggregates are coalesced into one file; the big cleaned table keeps
    its partitions so Spark writes in parallel.
    """
    writer_df = df.coalesce(coalesce) if coalesce else df
    writer = writer_df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(str(path))
