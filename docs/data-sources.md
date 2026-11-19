# Data Sources

Reference for the Data Source Layer (`src/ingest/`). All endpoints, locations and variables are defined in [`src/ingest/config.py`](../src/ingest/config.py).

| # | Source | Access | Content | Status |
|---|---|---|---|---|
| 1 | aWATTar | REST/JSON, free, no key | Austrian day-ahead spot prices | ✅ implemented |
| 2 | Open-Meteo | REST/JSON, free, no key, CC BY 4.0 | Hourly weather, 10 AT/DE locations | ✅ implemented |
| 3 | ENTSO-E Transparency Platform | REST/XML, security token (in `.env`) | Actual generation per type, total load (AT, DE) | phase 2 |
| 4 | Public holidays (`holidays` package) | Python library | AT and DE holidays as demand features | phase 2 |

---

## 1. aWATTar – day-ahead prices

- **Docs:** https://www.awattar.at/services/api
- **Endpoint:** `GET https://api.awattar.at/v1/marketdata?start=<epoch ms>&end=<epoch ms>`
- **Response:**
  ```json
  {"object": "list", "url": "/at/v1/marketdata",
   "data": [{"start_timestamp": 1714521600000, "end_timestamp": 1714525200000,
             "marketprice": 49.47, "unit": "Eur/MWh"}]}
  ```
- **History:** available at least back to 2019. Tomorrow's prices are published once a day, around 13:00 CET.
- **Fair use:** ~100 requests/day. The API returns a **full year in one request**, so we fetch one request per year. The full history 2019–today is 8 requests.
- **Cache:** `data/raw/awattar/YYYY.json`. The current year is always re-fetched.
- **Resolution:** the EPEX day-ahead market switched to 15-minute products on 1 Oct 2025, but aWATTar still returns **hourly** values (verified for Nov 2025). Each record carries `resolution_min`, so a change would be visible immediately.
- **Negative prices:** they occur and are valid. This is the core of our topic.

## 2. Open-Meteo – weather

- **Docs:** https://open-meteo.com/en/docs
- **Three endpoints, three purposes:**

  | Kind (`--kind`) | Endpoint | Content | Used for |
  |---|---|---|---|
  | `archive` | `archive-api.open-meteo.com/v1/archive` | Actual past weather (ERA5 reanalysis), lags ~5 days | Exploration, correlations |
  | `historical_forecast` | `historical-forecast-api.open-meteo.com/v1/forecast` | What weather models *forecast* back then (≈ 2022 onward) | **ML training** – the model must learn from forecasts, as it will in real use |
  | `forecast` | `api.open-meteo.com/v1/forecast` | Live forecast (we request 3 days) | "Tomorrow's cheap hours" demo |

- **Parameters:** `latitude`, `longitude` (comma-separated: all 10 locations in one request), `hourly=<variables>`, `start_date`, `end_date` (inclusive), `timezone=UTC`.
- **Response:** one object per location, in the order requested. The `hourly` block is column-oriented (one list per variable) and maps directly to a DataFrame.
- **Rate limits (free tier):** 600 calls/min, 5,000/hour, 10,000/day. Large requests count as several calls (per location, per 2 weeks, per 10 variables), so one year × 10 locations is a few hundred calls. `http.py` waits 65 s on HTTP 429.
- **Cache:** `data/raw/openmeteo/<kind>/YYYY.json`

### Variables

| Variable | Unit | Why |
|---|---|---|
| `temperature_2m` | °C | Heating/cooling demand |
| `shortwave_radiation` | W/m² | Global irradiance → solar output |
| `direct_radiation` | W/m² | Direct component of irradiance |
| `cloud_cover` | % | Solar output |
| `wind_speed_10m` | km/h | Surface wind |
| `wind_speed_100m` | km/h | Hub height → wind turbine output |
| `precipitation` | mm | Run-of-river hydro (delayed effect) |

### Locations

| Key | Country | Lat | Lon | Why |
|---|---|---|---|---|
| vienna | AT | 48.21 | 16.37 | Largest AT demand centre |
| parndorf | AT | 47.99 | 16.86 | AT wind cluster (Burgenland) |
| linz | AT | 48.31 | 14.29 | Industry, Upper Austria |
| graz | AT | 47.07 | 15.44 | Southern AT, solar |
| innsbruck | AT | 47.27 | 11.39 | Alpine, hydro region |
| hamburg | DE | 53.55 | 10.00 | DE north, onshore wind |
| husum | DE | 54.48 | 9.05 | DE North Sea coast wind |
| berlin | DE | 52.52 | 13.40 | DE north-east, wind + solar |
| munich | DE | 48.14 | 11.58 | DE south, solar |
| stuttgart | DE | 48.78 | 9.18 | DE south-west, solar |

**Why German weather?** Austria and Germany were one price zone until October 2018 and remain strongly interconnected, so German solar and wind output strongly influences Austrian prices.

## 3. ENTSO-E (phase 2)

- **Endpoint:** `https://web-api.tp.entsoe.eu/api` (token in `.env` as `ENTSOE_TOKEN`)
- **Areas:** AT `10YAT-APG------L`, DE-LU `10Y1001A1001A82H`
- **Planned:** actual generation per production type (A75), actual total load (A65). Client: `entsoe-py`.

## Raw cache format

Every cached file wraps the **unmodified** API response with metadata (data lineage):

```json
{"source": "awattar", "fetched_at": "2026-11-19T13:40:00+00:00",
 "params": {"start": 1546300800000, "end": 1577836800000, "year": 2019},
 "payload": { ...exact API response... }}
```

Open-Meteo files also contain `location_keys` (the order of locations in `payload`).

## Data quality checks (planned in notebook 01)

- Completeness: expected vs. actual hours per year and source. First check (fetch of 2026-11-19), all complete with no gaps and no null values:

  | Data | Period | Rows | Raw cache |
  |---|---|---|---|
  | aWATTar prices | 2019-01-01 → 2026-11-20 22:00 UTC | 69,143 hours | 9 MB |
  | Open-Meteo archive | 2019-01-01 → 2026-11-13 (archive lags ~6 days) | 689,760 (10 locations) | 38 MB |
  | Open-Meteo historical forecast | 2022-01-01 → 2026-11-13 | 426,720 (10 locations) | 24 MB |
- Daylight saving time: avoided by working in UTC throughout; checked for duplicates and gaps anyway
- Outliers: price spikes (e.g. the 2022 energy crisis), implausible weather values
- Coverage: historical forecasts only from ≈ 2022 → the ML training period starts there
