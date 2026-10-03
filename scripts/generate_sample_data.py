"""Generate a small SYNTHETIC sample that mimics the NYC Yellow Taxi schema.

It is only for testing the pipeline without downloading real data. It
deliberately includes dirty rows (negative fares, zero distances, dropoff before
pickup, duplicates, missing passenger counts) so the cleaning step has work to do.

Pandas/NumPy are fine here: this is a one-off 20k-row generator, not the pipeline.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import SAMPLE_DIR, SAMPLE_FILE  # noqa: E402

ZONES = [
    (132, "Queens", "JFK Airport"), (138, "Queens", "LaGuardia Airport"),
    (161, "Manhattan", "Midtown Center"), (162, "Manhattan", "Midtown East"),
    (163, "Manhattan", "Midtown North"), (164, "Manhattan", "Midtown South"),
    (230, "Manhattan", "Times Sq/Theatre District"), (236, "Manhattan", "Upper East Side North"),
    (237, "Manhattan", "Upper East Side South"), (186, "Manhattan", "Penn Station/Madison Sq West"),
    (170, "Manhattan", "Murray Hill"), (48, "Manhattan", "Clinton East"),
    (68, "Manhattan", "East Chelsea"), (79, "Manhattan", "East Village"),
    (107, "Manhattan", "Gramercy"), (113, "Manhattan", "Greenwich Village North"),
    (114, "Manhattan", "Greenwich Village South"), (142, "Manhattan", "Lincoln Square East"),
    (143, "Manhattan", "Lincoln Square West"), (148, "Manhattan", "Lower East Side"),
    (234, "Manhattan", "Union Sq"), (249, "Manhattan", "West Village"),
    (90, "Manhattan", "Flatiron"), (100, "Manhattan", "Garment District"),
    (141, "Manhattan", "Lenox Hill West"), (239, "Manhattan", "Upper West Side South"),
]
AIRPORTS = {132, 138}
HOUR_WEIGHTS = np.array([3, 2, 1.5, 1, 1, 1.5, 3, 5, 6, 5, 4.5, 4.5, 5, 5, 5.5, 6, 6.5, 8, 9, 8, 7, 6, 5, 4], float)


def generate(rows: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ids = np.array([z[0] for z in ZONES])
    zone_w = rng.dirichlet(np.ones(len(ids)) * 2.0)

    days = rng.integers(0, 91, rows)  # 2024-01-01 .. 2024-03-31
    hours = rng.choice(24, rows, p=HOUR_WEIGHTS / HOUR_WEIGHTS.sum())
    pickup = (pd.Timestamp("2024-01-01") + pd.to_timedelta(days, "D") + pd.to_timedelta(hours, "h")
              + pd.to_timedelta(rng.integers(0, 3600, rows), "s"))

    pu, do = rng.choice(ids, rows, p=zone_w), rng.choice(ids, rows, p=zone_w)
    airport = np.isin(pu, list(AIRPORTS)) | np.isin(do, list(AIRPORTS))
    distance = np.clip(rng.lognormal(0.85, 0.75, rows), 0.1, 35) + airport * rng.uniform(6, 14, rows)
    speed = np.clip(rng.normal(12, 3, rows) + (hours < 6) * 5, 4, 35)
    duration = distance / speed * 60 + 3 + rng.uniform(0, 4, rows)
    dropoff = pickup + pd.to_timedelta(duration * 60, "s")

    fare = np.round(3.0 + 2.7 * distance + 0.25 * duration, 2)
    fare = np.where(np.isin(pu, [132]) | np.isin(do, [132]), 70.0, fare)
    payment = rng.choice([1, 2, 3, 4], rows, p=[0.72, 0.24, 0.02, 0.02])
    tip = np.where(payment == 1, np.round(fare * rng.uniform(0.0, 0.3, rows), 2), 0.0)
    tolls = np.where(rng.random(rows) < 0.05, 6.94, 0.0)
    extra = rng.choice([0.0, 0.5, 1.0, 2.5, 3.5], rows)
    airport_fee = np.where(airport, 1.75, 0.0)
    total = np.round(fare + extra + 0.5 + tip + tolls + 0.3 + 2.5 + airport_fee, 2)

    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2], rows, p=[0.3, 0.7]).astype("int64"),
        "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": dropoff,
        "passenger_count": pd.array(rng.choice([1, 2, 3, 4, 5, 6], rows, p=[.7, .15, .05, .03, .05, .02]), dtype="Int64"),
        "trip_distance": np.round(distance, 2),
        "RatecodeID": np.where(airport, 2, 1).astype("int64"),
        "store_and_fwd_flag": "N", "PULocationID": pu.astype("int64"), "DOLocationID": do.astype("int64"),
        "payment_type": payment.astype("int64"), "fare_amount": fare, "extra": extra, "mta_tax": 0.5,
        "tip_amount": tip, "tolls_amount": tolls, "improvement_surcharge": 0.3, "total_amount": total,
        "congestion_surcharge": 2.5, "airport_fee": airport_fee,
    })

    # ---- inject realistic dirt (~5% of rows) ----
    def pick(frac: float) -> np.ndarray:
        return rng.choice(rows, int(rows * frac), replace=False)

    df.loc[pick(0.008), ["fare_amount", "total_amount"]] *= -1
    df.loc[pick(0.008), "trip_distance"] = 0.0
    bad = pick(0.006)
    df.loc[bad, "tpep_dropoff_datetime"] = df.loc[bad, "tpep_pickup_datetime"] - pd.Timedelta(minutes=5)
    df.loc[pick(0.01), "passenger_count"] = pd.NA
    df.loc[pick(0.01), "passenger_count"] = 0
    df.loc[pick(0.002), "trip_distance"] = 1000.0
    df.loc[pick(0.002), "payment_type"] = pd.NA
    dupes = df.sample(int(rows * 0.005), random_state=seed)
    return pd.concat([df, dupes], ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20000)
    args = parser.parse_args()

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    df = generate(args.rows)
    df.to_parquet(SAMPLE_FILE, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    pd.DataFrame(ZONES, columns=["LocationID", "Borough", "Zone"]).assign(service_zone="Yellow Zone") \
        .to_csv(SAMPLE_DIR / "taxi_zone_lookup.csv", index=False)
    print(f"Wrote {len(df):,} rows -> {SAMPLE_FILE}")


if __name__ == "__main__":
    main()
