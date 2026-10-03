"""Download NYC TLC Yellow Taxi monthly Parquet files + the zone lookup table.

Examples:
    python scripts/download_data.py --year 2024 --months 1 2 3
    python scripts/download_data.py --year 2023 --months 1-6
"""
from __future__ import annotations

import argparse
import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import RAW_DIR  # noqa: E402

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
INFO_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"


def parse_months(values: list[str]) -> list[int]:
    months: list[int] = []
    for value in values:
        if "-" in value:
            start, end = value.split("-")
            months.extend(range(int(start), int(end) + 1))
        else:
            months.append(int(value))
    invalid = [m for m in months if not 1 <= m <= 12]
    if invalid:
        raise ValueError(f"Invalid month(s): {invalid}")
    return sorted(set(months))


def download(url: str, target: Path) -> bool:
    if target.exists():
        print(f"  already exists, skipping: {target.name}")
        return True
    print(f"  downloading {target.name} ...")
    tmp = target.with_suffix(target.suffix + ".part")
    try:
        with urllib.request.urlopen(url, timeout=60) as response, open(tmp, "wb") as out:
            while chunk := response.read(1024 * 1024):
                out.write(chunk)
        tmp.rename(target)
        print(f"  saved ({target.stat().st_size / 1e6:.1f} MB)")
        return True
    except (urllib.error.URLError, TimeoutError) as exc:
        tmp.unlink(missing_ok=True)
        print(f"  FAILED: {exc}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--months", nargs="+", default=["1", "2", "3"], help="e.g. 1 2 3  or  1-6")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Source: {INFO_URL}\nSaving to: {RAW_DIR}")
    ok = download(ZONE_URL, RAW_DIR / "taxi_zone_lookup.csv")
    for month in parse_months(args.months):
        ok &= download(BASE_URL.format(year=args.year, month=month),
                       RAW_DIR / f"yellow_tripdata_{args.year}-{month:02d}.parquet")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
