"""Source 1: aWATTar day-ahead prices for Austria.

Usage from the command line (from the project root):
    python -m src.ingest.awattar --start 2019-01-01 --end 2026-12-31

One request per calendar year (the API returns a full year, 8,760 hours, in one
response), cached as data/raw/awattar/YYYY.json. Cached years are not fetched
again (use --refresh to force); the current year is always refreshed.
The full history 2019-today is ~8 requests, well within aWATTar's fair use
(~100 requests/day).
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import pandas as pd

from .config import AWATTAR_URL, RAW_DIR
from .http import get_json

CACHE_DIR = RAW_DIR / "awattar"


def _utc(ts: str | pd.Timestamp) -> pd.Timestamp:
    """Parse to a UTC timestamp; naive input is interpreted as UTC."""
    ts = pd.Timestamp(ts)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _to_ms(ts: pd.Timestamp) -> int:
    return int(ts.timestamp() * 1000)


def fetch_range(start: str | pd.Timestamp, end: str | pd.Timestamp) -> dict:
    """One raw API call for [start, end). Naive dates are interpreted as UTC."""
    start, end = _utc(start), _utc(end)
    return get_json(AWATTAR_URL, params={"start": _to_ms(start), "end": _to_ms(end)})


def fetch_year(year: int, refresh: bool = False) -> dict:
    """Fetch one calendar year (UTC), using the local cache when available.

    The cached file wraps the unmodified API response with fetch metadata
    (raw zone: we never alter what the source sent us).
    """
    path = CACHE_DIR / f"{year}.json"
    current_year = datetime.now(timezone.utc).year
    if path.exists() and not refresh and year != current_year:
        return json.loads(path.read_text(encoding="utf-8"))

    start = pd.Timestamp(f"{year}-01-01", tz="UTC")
    end = pd.Timestamp(f"{year + 1}-01-01", tz="UTC")
    payload = fetch_range(start, end)
    record = {
        "source": "awattar",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "params": {"start": _to_ms(start), "end": _to_ms(end), "year": year},
        "payload": payload,
    }
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=1), encoding="utf-8")
    return record


def fetch_history(start: str, end: str, refresh: bool = False) -> list[dict]:
    """Raw records for all years touching [start, end)."""
    first, last = _utc(start).year, (_utc(end) - pd.Timedelta(seconds=1)).year
    records = []
    for year in range(first, last + 1):
        cached = (CACHE_DIR / f"{year}.json").exists()
        current = year == datetime.now(timezone.utc).year
        print(f"aWATTar {year}: {'cache' if cached and not refresh and not current else 'API'}")
        records.append(fetch_year(year, refresh=refresh))
    return records


def to_dataframe(records: list[dict] | dict) -> pd.DataFrame:
    """Flatten raw records into a tidy DataFrame (UTC timestamps, EUR/MWh)."""
    if isinstance(records, dict):
        records = [records]
    rows = []
    for rec in records:
        payload = rec.get("payload", rec)  # accept wrapped cache records or a bare API response
        rows.extend(payload["data"])
    if not rows:
        return pd.DataFrame(columns=["ts", "ts_end", "price_eur_mwh", "resolution_min"])
    df = pd.DataFrame(rows)
    out = pd.DataFrame({
        "ts": pd.to_datetime(df["start_timestamp"], unit="ms", utc=True),
        "ts_end": pd.to_datetime(df["end_timestamp"], unit="ms", utc=True),
        "price_eur_mwh": df["marketprice"].astype(float),
    })
    out["resolution_min"] = ((out["ts_end"] - out["ts"]).dt.total_seconds() / 60).astype(int)
    assert (df["unit"].str.lower() == "eur/mwh").all(), "unexpected unit in aWATTar data"
    return out.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)


def load_prices(start: str, end: str, refresh: bool = False) -> pd.DataFrame:
    """Convenience: prices for [start, end) as a DataFrame (fetches/caches whole years)."""
    df = to_dataframe(fetch_history(start, end, refresh=refresh))
    return df[(df["ts"] >= _utc(start)) & (df["ts"] < _utc(end))].reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch aWATTar day-ahead prices into data/raw/awattar")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (exclusive)")
    ap.add_argument("--refresh", action="store_true", help="ignore cache")
    args = ap.parse_args()
    df = load_prices(args.start, args.end, refresh=args.refresh)
    print(f"{len(df)} price rows from {df['ts'].min()} to {df['ts'].max()}")


if __name__ == "__main__":
    main()
