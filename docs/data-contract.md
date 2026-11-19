# Data Contract: Kafka Topics

This is the interface between the **Data Source Layer** (producers in `src/ingest/`) and the **Storage/Processing Layer** (Spark consumer). Changes to topics, keys or fields must be agreed by both sides and updated here.

## Rules

- **One message per measurement**, value is UTF-8 JSON.
- **All timestamps are ISO-8601 in UTC** (`2024-05-01T00:00:00+00:00`). Local time (Europe/Vienna) is only used for display.
- **The key is the natural id** of a measurement. The same key can arrive more than once (backfill re-runs, price corrections). Consumers must **upsert by key**, never blindly insert.
- **Missing values are JSON `null`**, never `NaN`.
- **Every message has lineage fields:** `source` (which API/endpoint) and `ingested_at` (when it was published).
- Column names in `snake_case`, units in the name where not obvious (`price_eur_mwh`).
- Topics are kept forever (`retention = -1`), so consumers can re-read from offset 0.

## Topics

### `awattar.prices`

| | |
|---|---|
| Key | `ts` (e.g. `2024-05-01T00:00:00+00:00`) |
| Producer | `stream backfill awattar`, `stream live` (only new/changed prices) |

| Field | Type | Unit | Description |
|---|---|---|---|
| `ts` | string (timestamp) | UTC | Start of the delivery interval |
| `ts_end` | string (timestamp) | UTC | End of the delivery interval |
| `price_eur_mwh` | float | EUR/MWh | Day-ahead price, can be negative |
| `resolution_min` | int | minutes | Interval length (currently always 60) |
| `source` | string | | `awattar` |
| `ingested_at` | string (timestamp) | UTC | Publish time |

Example:
```json
{"ts": "2024-05-01T00:00:00+00:00", "ts_end": "2024-05-01T01:00:00+00:00",
 "price_eur_mwh": 49.47, "resolution_min": 60,
 "source": "awattar", "ingested_at": "2026-11-19T13:28:11+00:00"}
```

### `openmeteo.archive`, `openmeteo.historical_forecast`, `openmeteo.forecast`

| | |
|---|---|
| Key | `location\|ts` (e.g. `vienna\|2024-01-01T00:00:00+00:00`) |
| Producer | `stream backfill openmeteo --kind archive / historical_forecast`, `stream live` (forecast) |

| Field | Type | Unit | Description |
|---|---|---|---|
| `ts` | string (timestamp) | UTC | Hour the values apply to |
| `location` | string | | Location key, see [data-sources.md](data-sources.md#locations) |
| `country` | string | | `AT` or `DE` |
| `temperature_2m` | float | °C | |
| `shortwave_radiation` | float | W/m² | Mean over the preceding hour |
| `direct_radiation` | float | W/m² | |
| `cloud_cover` | int | % | |
| `wind_speed_10m` | float | km/h | |
| `wind_speed_100m` | float | km/h | |
| `precipitation` | float | mm | Sum over the preceding hour |
| `source` | string | | `openmeteo_archive`, `openmeteo_historical_forecast` or `openmeteo_forecast` |
| `ingested_at` | string (timestamp) | UTC | Publish time |
| `issued_at` | string (timestamp) | UTC | **Only in `openmeteo.forecast`:** when this forecast was fetched |

**Forecast topic special case:** every live poll sends a new forecast for the same hours, so the same key appears many times with different values. Depending on the use case, the consumer keeps either the **latest** forecast per key (upsert) or **all** of them (key + `issued_at`, to analyse forecast quality).

Example:
```json
{"ts": "2024-01-01T00:00:00+00:00", "location": "berlin", "country": "DE",
 "temperature_2m": 5.0, "shortwave_radiation": 0.0, "direct_radiation": 0.0,
 "cloud_cover": 100, "wind_speed_10m": 14.1, "wind_speed_100m": 28.5, "precipitation": 0.0,
 "source": "openmeteo_archive", "ingested_at": "2026-11-19T13:28:12+00:00"}
```

## Consumer sketch (Spark Structured Streaming)

```python
from pyspark.sql.functions import col, from_json

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")        # localhost:9094 from the host
       .option("subscribe", "awattar.prices")
       .option("startingOffsets", "earliest")
       .load())
prices = raw.select(from_json(col("value").cast("string"), price_schema).alias("p")).select("p.*")
prices.writeStream.foreachBatch(upsert_to_mongo).option("checkpointLocation", "data/checkpoints/prices").start()
```

Spark needs the connector package `org.apache.spark:spark-sql-kafka-0-10_<scala version>:<spark version>`, matching the installed PySpark.
