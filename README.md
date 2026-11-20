*Final Project – Big Data Analytics*

# Free Power Hours

*Predicting Cheap and Negative Electricity Prices in Austria from the Weather*

Electricity on the Austrian day-ahead market is becoming more volatile and negative more often. On sunny, windy weekends, prices regularly drop below zero, while still evenings can be very expensive. We want to find out which weather patterns drive these price swings, and whether tomorrow's weather forecast is enough to predict the cheap hours. Then we translate the result into concrete savings for households with a dynamic tariff (EV charging, heat pump).

> **Key story:** "When the sun shines and the wind blows, electricity is free. Can we see it coming?"

## Team

| Name | Main Responsibility |
|---|---|
| Scheder Marcel | Data Source Layer (APIs, scraping, data quality) |
| Trappl Paul | Data Storage & Processing (MongoDB, Spark pipeline) |
| Yilmaz Tarik | Analytics & Presentation (ML model, visualizations, story) |

*GitHub Repository:* https://github.com/free-power-hours/free-power-hours

## Overview

**APIs → Python producers → Kafka → Spark Structured Streaming → MongoDB → Spark / pandas → ML → Jupyter notebooks**

| Layer | What | Status |
|---|---|---|
| ① Data Source | aWATTar prices, Open-Meteo weather (10 AT/DE locations); later ENTSO-E generation/load and holidays | ✅ aWATTar, Open-Meteo |
| Streaming | Kafka topics as the hand-over between data source and storage | ✅ producers |
| ② Storage | MongoDB time-series collections, Parquet | open |
| ③ Processing / Analytics | Spark (cleaning, joins, window features), classification "cheap/negative hour" + price regression | open |
| ④ Output | Notebooks, charts, live "tomorrow's cheap hours" demo, savings calculator | open |

| Documentation | Content |
|---|---|
| [docs/project-plan.md](docs/project-plan.md) | Procedure, expected output, milestones, risks |
| [docs/architecture.md](docs/architecture.md) | Diagram, which tool does what, why Kafka, how the producer works, infrastructure |
| [docs/data-contract.md](docs/data-contract.md) | Kafka topics, keys and message formats (the interface between the layers) |
| [docs/data-sources.md](docs/data-sources.md) | Endpoints, parameters, rate limits, locations, variables, quirks |
| [notebooks/](notebooks/) | The graded documentation: code, explanations and results per layer |

## Getting Started

### Option A – VS Code Dev Container (recommended)

Requirements: Docker Desktop and the VS Code extension **Dev Containers**.

1. Open the folder in VS Code → `F1` → **Dev Containers: Reopen in Container**. The first build takes a few minutes. It starts MongoDB, Kafka and Kafka UI, provides Python 3.12 and Java 17 (for PySpark), and installs `requirements.txt`.
2. Add the ENTSO-E token to `.env`. If the file is missing, it is created from `.env.example`.
3. Open `notebooks/01_data_sources.ipynb` and select the kernel **Python 3.12 (/usr/local/bin/python)**.

### Option B – Local Python + Docker services

```bash
cp .env.example .env              # add the ENTSO-E token (never commit .env)
docker compose up -d              # MongoDB (27017), Kafka (9094), Kafka UI (http://localhost:8080)
python -m venv .venv
.venv\Scripts\activate            # Windows  (macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
jupyter lab
```

### Fetching data

```bash
# 1) APIs → raw cache data/raw/ (re-runs make no API calls)
python -m src.ingest.awattar   --start 2019-01-01 --end 2027-01-01
python -m src.ingest.openmeteo --kind archive             --start 2019-01-01 --end 2026-12-31
python -m src.ingest.openmeteo --kind historical_forecast --start 2022-01-01 --end 2026-12-31

# 2) Raw cache → Kafka
python -m src.ingest.stream backfill awattar   --start 2019-01-01 --end 2027-01-01
python -m src.ingest.stream backfill openmeteo --kind archive             --start 2019-01-01 --end 2026-12-31
python -m src.ingest.stream backfill openmeteo --kind historical_forecast --start 2022-01-01 --end 2026-12-31

# 3) Live feed: new prices + forecasts every 15 min (Ctrl+C to stop)
python -m src.ingest.stream live --interval 900
```

### Training and live prediction

1. Run [notebooks/training.ipynb](notebooks/training.ipynb). It reads prices and historical forecasts from Kafka, compares three models and saves the best one to `models/cheap_hours`.
2. Start the live feed (step 3 above) and, in a second terminal, the scoring job:

```bash
python -m src.predict                    # scores every new forecast -> topic predictions.cheap_hours
python -m src.predict --once --from-beginning   # score the forecasts already in Kafka, then exit
```

Both use the same feature code ([src/features.py](src/features.py)), so the live model sees the same columns as in training. The job prints the upcoming hours with `p_negative >= 0.3` (change with `--threshold`).

3. Open [notebooks/live_prediction.ipynb](notebooks/live_prediction.ipynb) and run all cells: a chart of the next 7 days (probability per hour, actual prices where known) that refreshes every minute. Stop it with the notebook's Interrupt button.

## Repository Structure

```
docker-compose.yml   all services: MongoDB, Kafka, Kafka UI, dev container, optional Jupyter/Spark
.devcontainer/       VS Code Dev Container configuration
docs/                team documentation (architecture, data contract, data sources)
src/ingest/          Data Source Layer: config, API clients, Kafka producer, stream CLI
src/features.py      feature engineering shared by training and live scoring
src/predict.py       live scoring job: openmeteo.forecast -> model -> predictions.cheap_hours
notebooks/           01_data_sources, training, live_prediction, then 02_storage, 03_processing, 05_story
models/              trained Spark ML models (regenerated by the training notebook, git-ignored)
data/raw/            raw API responses with fetch metadata (cache, git-ignored)
data/curated/        cleaned data (Parquet, git-ignored)
```

## Required Components – Checklist

| Requirement | How we cover it |
|---|---|
| ≥ 2 data sources | aWATTar, Open-Meteo (implemented); ENTSO-E, holidays (phase 2) |
| Web API / scraping | Three REST APIs |
| NoSQL | MongoDB time-series collections |
| Spark | Structured Streaming (Kafka → MongoDB), window functions, MLlib |
| Machine learning | Classification (cheap or negative hour) + price regression |
| Streaming (extra) | Kafka topics + live producer + live scoring with Spark Structured Streaming |

## Libraries

Pinned in [requirements.txt](requirements.txt): `requests`, `pandas`, `numpy`, `matplotlib`, `python-dotenv`, `confluent-kafka`, `jupyterlab`, `ipykernel`, `nbconvert`, `pyspark` (the `spark-sql-kafka` connector is downloaded by Spark on first start).
To be added in phase 2: `pymongo`, `entsoe-py`, `holidays`, `pyarrow`, `scikit-learn`.
