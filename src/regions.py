"""Region profiles: units, thresholds and labels differ between NYC (miles/USD) and Mumbai (km/INR)."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.config import RAW_DIR, SAMPLE_DIR


@dataclass(frozen=True)
class Region:
    key: str
    label: str
    currency: str
    distance_unit: str
    distance_bins: tuple          # upper edges of the first 4 distance categories
    distance_labels: tuple        # 5 labels
    fare_bins: tuple              # upper edges of the first 3 fare categories
    fare_labels: tuple            # 4 labels
    hist_cap: int                 # last histogram bucket = "cap+ distance units"
    max_distance: float
    max_fare: float
    max_total: float
    max_speed: float              # distance units per hour
    max_duration: float           # minutes
    vendor_names: dict
    payment_names: dict
    zone_files: tuple


NYC = Region(
    key="nyc", label="NYC Yellow Taxi (TLC)", currency="USD", distance_unit="mi",
    distance_bins=(2, 5, 10, 20),
    distance_labels=("0-2 miles", "2-5 miles", "5-10 miles", "10-20 miles", "20+ miles"),
    fare_bins=(10, 25, 50), fare_labels=("Low (<$10)", "Medium ($10-25)", "High ($25-50)", "Premium ($50+)"),
    hist_cap=30, max_distance=100.0, max_fare=500.0, max_total=1000.0, max_speed=80.0, max_duration=180.0,
    vendor_names={1: "Creative Mobile Technologies", 2: "VeriFone / Curb Mobility", 6: "Myle Technologies", 7: "Helix"},
    payment_names={0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided"},
    zone_files=(RAW_DIR / "taxi_zone_lookup.csv", SAMPLE_DIR / "taxi_zone_lookup.csv"),
)

MUMBAI = Region(
    key="mumbai", label="Mumbai taxi (synthetic)", currency="INR", distance_unit="km",
    distance_bins=(3, 8, 16, 32),
    distance_labels=("0-3 km", "3-8 km", "8-16 km", "16-32 km", "32+ km"),
    fare_bins=(100, 250, 500), fare_labels=("Low (<₹100)", "Medium (₹100-250)", "High (₹250-500)", "Premium (₹500+)"),
    hist_cap=50, max_distance=150.0, max_fare=10000.0, max_total=15000.0, max_speed=100.0, max_duration=240.0,
    vendor_names={1: "Kaali-Peeli (Black & Yellow)", 2: "Ola", 3: "Uber", 4: "Meru"},
    payment_names={1: "Digital (UPI/Card)", 2: "Cash", 3: "Wallet", 4: "Dispute", 5: "Unknown"},
    zone_files=(SAMPLE_DIR / "mumbai_zone_lookup.csv",),
)

REGIONS: dict[str, Region] = {"nyc": NYC, "mumbai": MUMBAI}
