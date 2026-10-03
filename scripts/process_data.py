"""Run the full PySpark pipeline: ingest -> clean -> transform -> aggregate -> Parquet.

    python scripts/process_data.py --mode sample
    python scripts/process_data.py --mode full
    python scripts/process_data.py --mode mumbai
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.storagelevel import StorageLevel

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import analytics, sql_queries  # noqa: E402
from src.cleaning import clean_trips, quality_report  # noqa: E402
from src.config import DATA_MODE, MUMBAI_FILE, RAW_DIR, REGION_BY_MODE, SAMPLE_FILE, SQL_DIR, processed_dir  # noqa: E402
from src.regions import REGIONS  # noqa: E402
from src.ingestion import describe_dataframe, load_zone_lookup, read_trips  # noqa: E402
from src.spark_session import get_spark  # noqa: E402
from src.transformation import add_features  # noqa: E402
from src.utils import save_parquet, timed  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["sample", "full", "mumbai"], default=DATA_MODE)
    parser.add_argument("--input", type=Path, help="Override input file/folder")
    parser.add_argument("--explain", action="store_true", help="Print the Spark execution plan")
    args = parser.parse_args()

    region = REGIONS[REGION_BY_MODE[args.mode]]
    default_input = {"sample": SAMPLE_FILE, "full": RAW_DIR, "mumbai": MUMBAI_FILE}[args.mode]
    input_path = args.input or default_input
    out = processed_dir(args.mode)
    spark = None
    try:
        spark = get_spark(shuffle_partitions=8 if args.mode == "sample" else 32)

        with timed("1. Ingest"):
            raw = read_trips(spark, input_path)
            describe_dataframe(raw)

        with timed("2. Clean"):
            cleaned = clean_trips(raw, region)

        with timed("3. Feature engineering"):
            zones = load_zone_lookup(spark, region.zone_files)
            if zones is None:
                print("   (no zone lookup found - zone IDs will be shown instead of names)")
            df = add_features(cleaned, region, zones)
            # Cache: this DataFrame is reused by ~15 aggregations below.
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            total_rows = df.count()
            if total_rows == 0:
                raise ValueError("No rows left after cleaning. Check the input data.")
            print(f"   {total_rows:,} rows after cleaning")

        if args.explain:
            print("\n--- Execution plan: monthly aggregation ---")
            analytics.time_metrics(df, "pickup_year", "pickup_month").explain()

        with timed("4. Data quality report"):
            report = quality_report(spark, raw, total_rows, region)
            report.show(30, truncate=False)
            save_parquet(report, out / "dq_report")

        with timed("5. Write cleaned trips (partitioned Parquet)"):
            save_parquet(df.repartition("pickup_year", "pickup_month"), out / "cleaned_trips",
                         coalesce=None, partition_by=["pickup_year", "pickup_month"])

        with timed("6. Aggregations"):
            outputs = {
                "trip_cube": analytics.build_cube(df),
                "kpis": analytics.overall_kpis(df),
                "hourly_metrics": analytics.time_metrics(df, "pickup_hour"),
                "daily_metrics": analytics.time_metrics(df, "pickup_date"),
                "monthly_metrics": analytics.time_metrics(df, "pickup_year", "pickup_month"),
                "route_metrics": analytics.route_metrics(df),
                "location_metrics": analytics.location_metrics(df),
                "vendor_monthly": analytics.vendor_monthly_ranking(df),
                "distance_hist": analytics.distance_histogram(df, region.hist_cap),
                "duration_hist": analytics.duration_histogram(df),
                "scatter_sample": analytics.scatter_sample(df, total_rows),
            }
            summary, sample = analytics.anomaly_analysis(spark, df)
            outputs["anomaly_summary"], outputs["anomaly_sample"] = summary, sample
            for name, frame in outputs.items():
                save_parquet(frame, out / name)
                print(f"   saved {name}")

        with timed(f"7. Spark SQL ({len(sql_queries.load_queries(SQL_DIR))} queries)"):
            results = sql_queries.run_queries(spark, df, show=True)
            for name, frame in results.items():
                save_parquet(frame, out / "sql_results" / name)

        df.unpersist()
        print(f"\nDone. Processed data is in: {out}\nNow run: streamlit run dashboard/app.py")
        return 0
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
