# Project Guide

## How components communicate
`scripts/process_data.py` is the only entry point for Spark. It calls `ingestion` -> `cleaning` -> `transformation` ->
`analytics` / `sql_queries` and writes Parquet to `data/processed/<mode>/`. The Streamlit app only reads those Parquet files.

## Important PySpark transformations
- `read_trips`: per-file `select` + `cast` to a fixed schema, then `unionByName` (TLC schemas drift between months)
- `clean_trips`: `dropna`, `withColumn` + `when` imputation, one combined `filter`, `dropDuplicates`
- `add_features`: date functions, `when` chains for categories, `create_map` for code labels, broadcast `join` for zone names
- `build_cube`: `groupBy().agg()` to additive sums; `route_metrics` / `vendor_monthly_ranking`: `Window` + `rank()`

## Spark SQL
13 queries: busiest hours (GROUP BY/HAVING), weekday vs weekend (CASE WHEN), peak-hour share (CTE + RANK),
7-day moving average (window frame), month-over-month growth (LAG), top routes (RANK), pickup share (window over aggregate),
monthly vendor rank (PARTITION BY).

## Streamlit architecture
Single `dashboard/app.py`: load cube -> sidebar filters -> boolean mask -> `summarize()` (Pandas groupby + ratio recomputation)
-> Plotly charts per tab. `st.cache_data` caches Parquet reads.

## Push to GitHub
```bash
git init
git add .
git status        # confirm no .venv, __pycache__, data/raw, data/processed
git commit -m "Initial commit: taxi trip big data analytics"
git branch -M main
git remote add origin https://github.com/<you>/taxi-trip-analytics.git
git push -u origin main
```
`.gitignore` already excludes `.venv/`, `__pycache__/`, raw data, processed Parquet and Spark temp files (the small sample is kept).

## Portfolio
**Resume title:** Taxi Trip Big Data Analytics & Intelligence Dashboard (PySpark, Spark SQL, Streamlit)

**Bullets**
- Built a PySpark pipeline that ingests, cleans and aggregates multi-month NYC Yellow Taxi data (millions of rows) into partitioned Parquet.
- Wrote 13 Spark SQL queries and DataFrame window functions (RANK, LAG, moving averages) for route, revenue and vendor analysis.
- Designed a pre-aggregated "cube" so a Streamlit dashboard with 10 filters stays fast without loading trip-level data into Pandas.
- Implemented a data-quality report and documented domain-based cleaning rules (invalid fares, durations, duplicates, outliers).
- Applied Spark optimizations: caching, broadcast joins, column pruning, repartitioning and execution-plan inspection.

**GitHub description:** End-to-end big data analytics on NYC taxi trips with PySpark, Spark SQL and a Streamlit dashboard.

**Interview explanation:** "Spark does all heavy processing and writes Parquet aggregates; Streamlit only reads small aggregates.
I documented every cleaning rule and kept tip % to card payments because cash tips aren't recorded."

**Possible questions**
- Why PySpark instead of Pandas? What changes at 100M rows?
- What is lazy evaluation? What triggers a Spark job?
- Why `persist()` and when would it hurt?
- What is a broadcast join and why use it for the zone table?
- Why did you impute passenger_count instead of dropping rows?
- Why a pre-aggregated cube, and what are its limits (zone filters)?
- How would you handle schema changes between monthly files?
- Difference between `repartition` and `coalesce`?
