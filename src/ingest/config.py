"""Central configuration for the Data Source Layer.

Every endpoint, location and variable used by the project is defined here,
so the other layers (storage, processing, analytics) have a single source of truth.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CURATED_DIR = DATA_DIR / "curated"

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Secrets / connection settings (from .env, never hard-coded)
# ---------------------------------------------------------------------------
ENTSOE_TOKEN = os.getenv("ENTSOE_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "free_power_hours")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")

# ---------------------------------------------------------------------------
# Kafka topics = the hand-over point from the Data Source Layer to storage/processing.
# One message per measurement (JSON), key = natural id, so consumers can upsert.
# ---------------------------------------------------------------------------
TOPICS = {
    "awattar":                        "awattar.prices",                 # key: ts
    "openmeteo_archive":              "openmeteo.archive",              # key: location|ts
    "openmeteo_historical_forecast":  "openmeteo.historical_forecast",  # key: location|ts
    "openmeteo_forecast":             "openmeteo.forecast",             # key: location|ts (re-sent per forecast run)
}

# Generic client name; don't put personal contact details here.
USER_AGENT = "free-power-hours-student-project/0.1"

# ---------------------------------------------------------------------------
# Source 1: aWATTar - Austrian day-ahead spot prices
# Docs: https://www.awattar.at/services/api
# GET /v1/marketdata?start=<epoch ms>&end=<epoch ms>
# Response: {"object": "list", "data": [{start_timestamp, end_timestamp, marketprice, unit}]}
# Fair use: ~100 requests/day -> we fetch one month per request and cache it.
# ---------------------------------------------------------------------------
AWATTAR_URL = "https://api.awattar.at/v1/marketdata"
AWATTAR_HISTORY_START = "2019-01-01"

# ---------------------------------------------------------------------------
# Source 2: Open-Meteo - weather (no key needed, CC BY 4.0)
# Docs: https://open-meteo.com/en/docs
# ---------------------------------------------------------------------------
OPENMETEO_URLS = {
    # Actual historical weather (ERA5 reanalysis) - for exploratory analysis
    "archive": "https://archive-api.open-meteo.com/v1/archive",
    # What the forecast models predicted back then (~2022 onwards) - for ML training
    "historical_forecast": "https://historical-forecast-api.open-meteo.com/v1/forecast",
    # Live forecast (up to 16 days ahead) - for the "tomorrow's cheap hours" demo
    "forecast": "https://api.open-meteo.com/v1/forecast",
}
# Days of live forecast we request (API max 16). 7 = one week for the live prediction chart;
# beyond ~3-4 days weather forecasts become noticeably less reliable.
OPENMETEO_FORECAST_DAYS = 7

# Hourly variables we request. Units are Open-Meteo defaults.
WEATHER_VARIABLES = [
    "temperature_2m",        # deg C - drives heating/cooling demand
    "shortwave_radiation",   # W/m2 - global horizontal irradiance -> solar output
    "direct_radiation",      # W/m2
    "cloud_cover",           # %
    "wind_speed_10m",        # km/h
    "wind_speed_100m",       # km/h - hub height, relevant for wind turbines
    "precipitation",         # mm - run-of-river hydro (with delay)
]

# Locations: AT demand centres + wind region, DE solar south + wind north.
# key -> (country, latitude, longitude, why)
LOCATIONS = {
    "vienna":     ("AT", 48.21, 16.37, "largest AT demand centre"),
    "parndorf":   ("AT", 47.99, 16.86, "AT wind cluster (Burgenland)"),
    "linz":       ("AT", 48.31, 14.29, "industry, Upper Austria"),
    "graz":       ("AT", 47.07, 15.44, "southern AT, solar"),
    "innsbruck":  ("AT", 47.27, 11.39, "alpine, hydro region"),
    "hamburg":    ("DE", 53.55, 10.00, "DE north, onshore wind"),
    "husum":      ("DE", 54.48,  9.05, "DE North Sea coast wind"),
    "berlin":     ("DE", 52.52, 13.40, "DE north-east, wind + solar"),
    "munich":     ("DE", 48.14, 11.58, "DE south, solar"),
    "stuttgart":  ("DE", 48.78,  9.18, "DE south-west, solar"),
}

# ---------------------------------------------------------------------------
# Source 3 (phase 2): ENTSO-E Transparency Platform
# Docs: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
# ---------------------------------------------------------------------------
ENTSOE_URL = "https://web-api.tp.entsoe.eu/api"
ENTSOE_AREAS = {"AT": "10YAT-APG------L", "DE_LU": "10Y1001A1001A82H"}
