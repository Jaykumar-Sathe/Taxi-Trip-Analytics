"""Central configuration. Every path can be overridden with an env variable."""
from __future__ import annotations

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("TAXI_DATA_DIR", ROOT_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "sample"
PROCESSED_DIR = DATA_DIR / "processed"
SQL_DIR = ROOT_DIR / "sql"

SAMPLE_FILE = SAMPLE_DIR / "taxi_sample.parquet"
ZONE_LOOKUP_CANDIDATES = [RAW_DIR / "taxi_zone_lookup.csv", SAMPLE_DIR / "taxi_zone_lookup.csv"]

DATA_MODE = os.getenv("DATA_MODE", "sample")  # "sample" or "full"


def processed_dir(mode: str) -> Path:
    """Processed output folder for a data mode (sample/full)."""
    return PROCESSED_DIR / mode

MUMBAI_FILE = SAMPLE_DIR / "mumbai_taxi_sample.parquet"
# data mode -> region profile (see src/regions.py)
REGION_BY_MODE = {"sample": "nyc", "full": "nyc", "mumbai": "mumbai"}
