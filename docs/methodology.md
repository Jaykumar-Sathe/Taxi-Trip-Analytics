# Methodology

## Cleaning rules (src/cleaning.py)

| Rule | Why |
|---|---|
| Drop rows with null timestamps, distance, fare, total or zones | These fields are required for every metric; they cannot be guessed |
| `passenger_count` null or 0 -> 1 | Driver-entered field; the trip and its revenue are still valid, so it is imputed instead of deleted |
| Null `payment_type` -> 5 (Unknown) | Keeps the revenue; shown as its own category |
| Pickup date must be 2015-2030 | Raw TLC files contain stray rows with dates like 2002 or 2098 |
| Duration 1-180 min, drop-off after pickup | Removes zero/negative durations and meter left running |
| Distance in (0, 100] miles | 0 = cancelled/glitch; >100 miles is not a city taxi trip |
| Fare in (0, 500], total in (0, 1000] | Negative values are refunds/adjustments, huge values are errors |
| Passenger count <= 6 | Legal vehicle capacity |
| Speed <= 80 mph | Physically implausible in NYC |
| Drop exact duplicates on 8 key columns | Repeated records inflate revenue |

Filters are **domain based**. Statistical outliers (e.g. a very expensive but real airport trip) are not removed automatically.
The anomaly step only *reports* z-score outliers (|z| > 4) on already-cleaned data.

## Known limitations
- Cash tips are not recorded, so tip % is calculated on credit-card trips only.
- `passenger_count` is driver-entered and imputed when missing.
- The bundled sample is **synthetic** (schema-faithful, for testing); the key insights below only apply to real data after you run it.
- Dashboard zone filters apply to the Locations tab only.

## Data quality (sample dataset, 20,100 raw rows)
Run `python scripts/process_data.py --mode sample` and open the **Data Quality & SQL** tab, or read `data/processed/sample/dq_report`. Real-data numbers will differ.
