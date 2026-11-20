"""Source 2: Open-Meteo hourly weather for all project locations.

Usage from the command line (from the project root):
    python -m src.ingest.openmeteo --kind archive --start 2019-01-01 --end 2019-12-31
    python -m src.ingest.openmeteo --kind historical_forecast --start 2023-01-01 --end 2023-12-31
    python -m src.ingest.openmeteo --kind forecast

All locations are requested in a single call (Open-Meteo accepts comma-separated
coordinates). Historical data is fetched one calendar year per request and cached
as data/raw/openmeteo/<kind>/<year>.json.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import pandas as pd

from .config import LOCATIONS, OPENMETEO_FORECAST_DAYS, OPENMETEO_URLS, RAW_DIR, WEATHER_VARIABLES
from .http import get_json

CACHE_DIR = RAW_DIR / "openmeteo"


def build_params(start: str | None = None, end: str | None = None,
                 locations: dict = LOCATIONS, variables: list[str] = WEATHER_VARIABLES) -> dict:
    """Query parameters for one multi-location request. Dates are inclusive (YYYY-MM-DD)."""
    params = {
        "latitude": ",".join(str(v[1]) for v in locations.values()),
        "longitude": ",".join(str(v[2]) for v in locations.values()),
        "hourly": ",".join(variables),
        "timezone": "UTC",
    }
    if start:
        params["start_date"] = start
    if end:
        params["end_date"] = end
    return params


def fetch(kind: str, start: str | None = None, end: str | None = None,
          locations: dict = LOCATIONS, variables: list[str] = WEATHER_VARIABLES) -> dict:
    """One raw API call, wrapped with metadata. `kind` is a key of OPENMETEO_URLS."""
    params = build_params(start, end, locations, variables)
    if kind == "forecast":
        params["forecast_days"] = OPENMETEO_FORECAST_DAYS
    payload = get_json(OPENMETEO_URLS[kind], params=params)
    if isinstance(payload, dict):  # single location -> API returns an object, not a list
        payload = [payload]
    return {
        "source": f"openmeteo_{kind}",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "params": params,
        "location_keys": list(locations),  # response order == request order
        "payload": payload,
    }


def fetch_year(kind: str, year: int, refresh: bool = False) -> dict:
    """Fetch one calendar year for all locations, cached on disk."""
    path = CACHE_DIR / kind / f"{year}.json"
    current_year = datetime.now(timezone.utc).year
    if path.exists() and not refresh and year != current_year:
        return json.loads(path.read_text(encoding="utf-8"))

    end = f"{year}-12-31"
    if year == current_year:  # archive lags a few days behind today
        end = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=6)).strftime("%Y-%m-%d")
    record = fetch(kind, f"{year}-01-01", end)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")
    return record


def fetch_history(kind: str, start_year: int, end_year: int, refresh: bool = False) -> list[dict]:
    records = []
    for year in range(start_year, end_year + 1):
        print(f"Open-Meteo {kind} {year}")
        records.append(fetch_year(kind, year, refresh=refresh))
    return records


def to_dataframe(records: list[dict] | dict) -> pd.DataFrame:
    """Flatten raw records into long format: one row per (ts, location)."""
    if isinstance(records, dict):
        records = [records]
    frames = []
    for rec in records:
        for key, loc in zip(rec["location_keys"], rec["payload"]):
            df = pd.DataFrame(loc["hourly"])
            df["ts"] = pd.to_datetime(df.pop("time"), utc=True)
            df.insert(0, "location", key)
            df.insert(1, "country", LOCATIONS.get(key, ("?",))[0])
            frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    cols = ["ts", "location", "country"] + [c for c in out.columns if c not in ("ts", "location", "country")]
    return out[cols].drop_duplicates(["ts", "location"]).sort_values(["ts", "location"]).reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Open-Meteo weather into data/raw/openmeteo")
    ap.add_argument("--kind", choices=list(OPENMETEO_URLS), default="archive")
    ap.add_argument("--start", help="YYYY-MM-DD (year is used for historical kinds)")
    ap.add_argument("--end", help="YYYY-MM-DD")
    ap.add_argument("--refresh", action="store_true")
    args = ap.parse_args()
    if args.kind == "forecast":
        df = to_dataframe(fetch("forecast"))
    else:
        recs = fetch_history(args.kind, int(args.start[:4]), int(args.end[:4]), refresh=args.refresh)
        df = to_dataframe(recs)
    print(f"{len(df)} rows, {df['location'].nunique()} locations, {df['ts'].min()} -> {df['ts'].max()}")


if __name__ == "__main__":
    main()
