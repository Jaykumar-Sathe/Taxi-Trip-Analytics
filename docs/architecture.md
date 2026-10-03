# Architecture

```text
RAW PARQUET (data/raw or data/sample)
        │  src/ingestion.py      explicit schema, per-file cast, union
        ▼
CLEANING (src/cleaning.py)       domain rules + dedupe + quality report
        ▼
FEATURES (src/transformation.py) time, speed, cost, categories, zone join (broadcast)
        ▼  .persist()
┌───────┴────────────────────────────────┐
│ analytics.py (DataFrame API + windows) │  sql_queries.py + sql/*.sql (Spark SQL)
└───────┬────────────────────────────────┘
        ▼
PARQUET (data/processed/<mode>/...)
        ▼  pandas.read_parquet (small aggregates only)
STREAMLIT (dashboard/app.py): sidebar filters → KPIs → 7 tabs → CSV downloads
```

**Why a "cube"?** Filters need to be dynamic, but re-running Spark on every click is slow and
loading millions of rows into Pandas defeats the purpose. Spark pre-aggregates trips by
date x hour x vendor x payment x distance category x passenger count and stores only additive
sums. The dashboard filters this small table and re-computes averages from the sums.
Zone filters apply only to the Locations tab because adding zones to the cube would explode its size.

**Components talk only through files:** `scripts/process_data.py` (Spark) writes Parquet;
`dashboard/app.py` reads Parquet. The dashboard never starts Spark.
