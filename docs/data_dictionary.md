# Data Dictionary

Source: NYC TLC Yellow Taxi Trip Records. Official page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Raw columns

| Column | Type | Meaning |
|---|---|---|
| VendorID | int | Technology provider (1 = Creative Mobile Technologies, 2 = VeriFone / Curb Mobility, 6 = Myle, 7 = Helix) |
| tpep_pickup_datetime | timestamp | Meter engaged |
| tpep_dropoff_datetime | timestamp | Meter disengaged |
| passenger_count | int | Passengers (driver-entered, often null/0) |
| trip_distance | double | Trip distance in miles from the taximeter |
| RatecodeID | int | Rate code (1 standard, 2 JFK, 3 Newark, 4 Nassau/Westchester, 5 negotiated, 6 group) |
| store_and_fwd_flag | string | Y/N: trip record held in vehicle memory before sending |
| PULocationID / DOLocationID | int | Pickup / drop-off taxi zone (join with `taxi_zone_lookup.csv`) |
| payment_type | int | 0 Flex fare, 1 Credit card, 2 Cash, 3 No charge, 4 Dispute, 5 Unknown, 6 Voided |
| fare_amount | double | Time-and-distance fare from the meter |
| extra | double | Rush hour / overnight surcharges |
| mta_tax | double | MTA tax |
| tip_amount | double | Tip (**card tips only; cash tips are not recorded**) |
| tolls_amount | double | Tolls paid |
| improvement_surcharge | double | Improvement surcharge |
| congestion_surcharge | double | NYS congestion surcharge |
| airport_fee | double | Airport pickup fee |
| total_amount | double | Total charged to passengers (excludes cash tips) |

## Engineered columns

| Column | Definition |
|---|---|
| trip_duration_minutes | (dropoff - pickup) in minutes |
| pickup_date / year / month / day | Date parts of pickup time |
| pickup_day_of_week | Day name (Monday..Sunday) |
| pickup_hour | 0-23 |
| is_weekend | Saturday or Sunday |
| average_speed_mph | trip_distance / (duration / 60) |
| fare_per_mile | fare_amount / trip_distance |
| tip_percentage | tip_amount / fare_amount * 100 |
| total_cost_per_mile | total_amount / trip_distance |
| time_period | Morning 05-11, Afternoon 12-16, Evening 17-21, Night otherwise |
| trip_distance_category | 0-2, 2-5, 5-10, 10-20, 20+ miles |
| fare_category | Low (<$10), Medium ($10-25), High ($25-50), Premium ($50+) |
| passenger_category | Solo, Pair, Group (3+) |
| vendor_name / payment_name | Readable labels for the codes |
| pickup_zone / dropoff_zone | Zone names from the lookup (falls back to "Zone <id>") |

## Processed outputs (`data/processed/<mode>/`)

| Folder | Content |
|---|---|
| cleaned_trips | Full cleaned + featured table, partitioned by year/month |
| trip_cube | Small pre-aggregate behind all dashboard filters |
| kpis, hourly_metrics, daily_metrics, monthly_metrics | Overall and time-based metrics |
| route_metrics, location_metrics | Route and zone metrics with RANK() window columns |
| vendor_monthly | Monthly vendor revenue ranking |
| distance_hist, duration_hist, scatter_sample | Chart data |
| dq_report, anomaly_summary, anomaly_sample | Data quality and anomaly results |
| sql_results | Output of every Spark SQL query |
