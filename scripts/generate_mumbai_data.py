"""Generate a SYNTHETIC Mumbai taxi dataset (same column schema as the NYC data, but in INR and km).

No public Mumbai trip-level dataset with this schema exists, so this simulates one:
  * real Mumbai localities with approximate coordinates (distances = haversine x road factor)
  * Kaali-Peeli (black & yellow) taxis with metered fares, plus app cabs (Ola / Uber / Meru) with surge
  * Mumbai-like demand peaks (morning and evening) and slower traffic at peak hours
Fare parameters are ILLUSTRATIVE, not official tariffs. Do not quote results as real findings.

    python scripts/generate_mumbai_data.py --rows 100000
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import MUMBAI_FILE, SAMPLE_DIR  # noqa: E402

# (name, area, lat, lon, demand weight)
LOCALITIES = [
    ("Colaba", "South Mumbai", 18.9067, 72.8147, 3), ("Nariman Point", "South Mumbai", 18.9256, 72.8242, 3),
    ("Churchgate", "South Mumbai", 18.9322, 72.8264, 4), ("Marine Drive", "South Mumbai", 18.9440, 72.8235, 2),
    ("CSMT / Fort", "South Mumbai", 18.9398, 72.8355, 5), ("Byculla", "South Mumbai", 18.9760, 72.8328, 2),
    ("Mahalaxmi", "South Mumbai", 18.9826, 72.8236, 2), ("Lower Parel", "Central Mumbai", 18.9954, 72.8301, 5),
    ("Worli", "Central Mumbai", 19.0176, 72.8156, 4), ("Dadar", "Central Mumbai", 19.0178, 72.8478, 6),
    ("Matunga", "Central Mumbai", 19.0270, 72.8570, 2), ("Sion", "Central Mumbai", 19.0390, 72.8619, 2),
    ("Mahim", "Western Suburbs", 19.0330, 72.8400, 2), ("Bandra West", "Western Suburbs", 19.0596, 72.8295, 7),
    ("Santacruz", "Western Suburbs", 19.0817, 72.8370, 4), ("Vile Parle", "Western Suburbs", 19.0990, 72.8440, 3),
    ("Juhu", "Western Suburbs", 19.1075, 72.8263, 3), ("Andheri West", "Western Suburbs", 19.1363, 72.8277, 7),
    ("Goregaon", "Western Suburbs", 19.1663, 72.8526, 4), ("Malad", "Western Suburbs", 19.1874, 72.8484, 4),
    ("Kandivali", "Western Suburbs", 19.2043, 72.8520, 3), ("Borivali", "Western Suburbs", 19.2307, 72.8567, 4),
    ("Bandra Kurla Complex", "Eastern Suburbs", 19.0690, 72.8685, 7), ("Kurla", "Eastern Suburbs", 19.0728, 72.8826, 4),
    ("Chembur", "Eastern Suburbs", 19.0522, 72.9005, 3), ("Ghatkopar", "Eastern Suburbs", 19.0860, 72.9081, 4),
    ("Powai", "Eastern Suburbs", 19.1176, 72.9060, 4), ("Vikhroli", "Eastern Suburbs", 19.1100, 72.9270, 2),
    ("Mulund", "Eastern Suburbs", 19.1726, 72.9425, 2), ("Andheri East", "Eastern Suburbs", 19.1136, 72.8697, 7),
    ("Mumbai Airport T2 (International)", "Airport", 19.0887, 72.8679, 8),
    ("Mumbai Airport T1 (Domestic)", "Airport", 19.0990, 72.8570, 4),
    ("Thane", "Thane / Navi Mumbai", 19.2183, 72.9781, 3), ("Vashi", "Thane / Navi Mumbai", 19.0771, 72.9986, 3),
]
AIRPORT_IDS = [30, 31]  # LocationID = index + 1
HOUR_WEIGHTS = np.array([2, 1.2, 1, 0.8, 1, 2, 4, 7, 10, 10, 7, 6, 6, 6, 6, 6.5, 7, 9, 11, 11, 9, 7, 5, 3], float)


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def generate(rows: int, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = len(LOCALITIES)
    lat = np.array([z[2] for z in LOCALITIES]); lon = np.array([z[3] for z in LOCALITIES])
    w = np.array([z[4] for z in LOCALITIES], float); w /= w.sum()

    # Gravity model: drop-off probability ~ demand weight x exp(-distance / 6 km), so most trips stay local.
    dist_matrix = haversine_km(lat[:, None], lon[:, None], lat[None, :], lon[None, :])
    prob = w[None, :] * np.exp(-dist_matrix / 6.0)
    for airport_id in AIRPORT_IDS:  # nobody takes a cab from a terminal back to the same terminal
        prob[airport_id - 1, airport_id - 1] = 0.0
    prob /= prob.sum(axis=1, keepdims=True)
    pu_i = rng.choice(n, rows, p=w)
    do_i = np.empty(rows, dtype=int)
    for i in range(n):
        idx = np.where(pu_i == i)[0]
        do_i[idx] = rng.choice(n, len(idx), p=prob[i])
    straight = haversine_km(lat[pu_i], lon[pu_i], lat[do_i], lon[do_i])
    same = pu_i == do_i
    distance = np.where(same, rng.uniform(0.8, 3.0, rows), np.maximum(straight * rng.uniform(1.25, 1.55, rows), 1.0))
    distance = np.round(distance, 2)

    days = rng.integers(0, 90, rows)  # 2026-01-01 .. 2026-03-31
    hours = rng.choice(24, rows, p=HOUR_WEIGHTS / HOUR_WEIGHTS.sum())
    pickup = (pd.Timestamp("2026-01-01") + pd.to_timedelta(days, "D") + pd.to_timedelta(hours, "h")
              + pd.to_timedelta(rng.integers(0, 3600, rows), "s"))
    peak = np.isin(hours, [8, 9, 10, 17, 18, 19, 20, 21])
    speed = np.clip(rng.normal(np.where(peak, 14, np.where((hours < 6), 26, 20)), 3), 6, 45)
    duration = distance / speed * 60 + 2 + rng.uniform(0, 3, rows)
    dropoff = pickup + pd.to_timedelta(duration * 60, "s")

    vendor = rng.choice([1, 2, 3, 4], rows, p=[0.30, 0.25, 0.35, 0.10])  # 1 = Kaali-Peeli
    taxi = vendor == 1
    taxi_fare = 28 + np.maximum(distance - 1.5, 0) * 19
    app_fare = 45 + 13 * distance + 1.6 * duration
    fare = np.round(np.where(taxi, taxi_fare, app_fare))
    surge = np.where(peak, rng.uniform(1.2, 1.8, rows), rng.uniform(1.0, 1.15, rows))
    night = hours < 5
    extra = np.where(taxi, np.where(night, np.round(fare * 0.25), 0.0), np.round(fare * (surge - 1)))

    airport = np.isin(pu_i + 1, AIRPORT_IDS) | np.isin(do_i + 1, AIRPORT_IDS)
    airport_fee = np.where(airport & ~taxi & np.isin(pu_i + 1, AIRPORT_IDS), 100.0, 0.0)
    tolls = np.where((distance > 12) & (rng.random(rows) < 0.2), 85.0, 0.0)  # e.g. sea-link style toll

    payment = np.where(taxi, rng.choice([1, 2, 3, 4], rows, p=[0.30, 0.65, 0.03, 0.02]),
                       rng.choice([1, 2, 3, 4], rows, p=[0.75, 0.15, 0.08, 0.02]))
    tip = np.where((payment == 1) & (rng.random(rows) < 0.2), rng.choice([10, 20, 30, 50], rows), 0.0).astype(float)
    total = fare + extra + tip + tolls + airport_fee

    pax_cap = np.where(taxi, 4, 6)
    passengers = np.minimum(rng.choice([1, 2, 3, 4, 5, 6], rows, p=[.55, .22, .10, .08, .03, .02]), pax_cap)

    df = pd.DataFrame({
        "VendorID": vendor.astype("int64"),
        "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": dropoff,
        "passenger_count": pd.array(passengers, dtype="Int64"),
        "trip_distance": distance, "RatecodeID": np.where(airport, 2, 1).astype("int64"),
        "store_and_fwd_flag": "N", "PULocationID": (pu_i + 1).astype("int64"), "DOLocationID": (do_i + 1).astype("int64"),
        "payment_type": payment.astype("int64"), "fare_amount": fare, "extra": extra, "mta_tax": 0.0,
        "tip_amount": tip, "tolls_amount": tolls, "improvement_surcharge": 0.0,
        "total_amount": total, "congestion_surcharge": 0.0, "airport_fee": airport_fee,
    })

    def pick(frac: float) -> np.ndarray:
        return rng.choice(rows, int(rows * frac), replace=False)

    df.loc[pick(0.008), ["fare_amount", "total_amount"]] *= -1
    df.loc[pick(0.008), "trip_distance"] = 0.0
    bad = pick(0.006)
    df.loc[bad, "tpep_dropoff_datetime"] = df.loc[bad, "tpep_pickup_datetime"] - pd.Timedelta(minutes=5)
    df.loc[pick(0.01), "passenger_count"] = pd.NA
    df.loc[pick(0.01), "passenger_count"] = 0
    df.loc[pick(0.002), "trip_distance"] = 1500.0
    df.loc[pick(0.002), "payment_type"] = pd.NA
    dupes = df.sample(int(rows * 0.005), random_state=seed)
    return pd.concat([df, dupes], ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    df = generate(args.rows)
    df.to_parquet(MUMBAI_FILE, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    pd.DataFrame([(i + 1, z[1], z[0]) for i, z in enumerate(LOCALITIES)],
                 columns=["LocationID", "Borough", "Zone"]).to_csv(SAMPLE_DIR / "mumbai_zone_lookup.csv", index=False)
    print(f"Wrote {len(df):,} rows -> {MUMBAI_FILE}")


if __name__ == "__main__":
    main()
