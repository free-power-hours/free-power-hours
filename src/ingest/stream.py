"""Stream data-source records into Kafka: historical backfill and a live polling loop.

Backfill (replays the cached raw files through the same pipeline as live data):
    python -m src.ingest.stream backfill awattar   --start 2019-01-01 --end 2026-11-01
    python -m src.ingest.stream backfill openmeteo --kind archive --start 2019-01-01 --end 2026-12-31

Live (polls the APIs and publishes new/changed values; Ctrl+C to stop):
    python -m src.ingest.stream live --interval 900
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import pandas as pd

from . import awattar, openmeteo
from .config import TOPICS
from .producer import make_producer, publish


def backfill_awattar(start: str, end: str) -> int:
    df = awattar.load_prices(start, end)
    n = publish(df, TOPICS["awattar"], ["ts"], source="awattar")
    print(f"-> {n} messages to {TOPICS['awattar']}")
    return n


def backfill_openmeteo(kind: str, start: str, end: str) -> int:
    producer = make_producer()
    topic = TOPICS[f"openmeteo_{kind}"]
    total = 0
    for year in range(int(start[:4]), int(end[:4]) + 1):
        df = openmeteo.to_dataframe(openmeteo.fetch_year(kind, year))
        total += publish(df, topic, ["location", "ts"], source=f"openmeteo_{kind}", producer=producer)
        print(f"   {year}: {len(df)} rows queued")
    producer.flush(120)
    print(f"-> {total} messages to {topic}")
    return total


def live(interval: int) -> None:
    """Poll aWATTar and the Open-Meteo forecast every `interval` seconds.

    * Prices: day-ahead prices for tomorrow appear once per day (~13:00 CET);
      only new or changed prices are sent.
    * Forecast: every poll is a new forecast run, sent with `issued_at`, so the
      history of forecasts for the same hour is kept in the stream.
    """
    producer = make_producer()
    seen_prices: dict[pd.Timestamp, float] = {}
    print(f"Live mode, polling every {interval}s. Ctrl+C to stop.")
    while True:
        now = pd.Timestamp.now(tz="UTC")
        try:
            prices = awattar.to_dataframe(awattar.fetch_range(now.floor("D") - pd.Timedelta(days=1),
                                                              now.floor("D") + pd.Timedelta(days=2)))
            new = prices[[seen_prices.get(ts) != p for ts, p in zip(prices["ts"], prices["price_eur_mwh"])]]
            n_p = publish(new, TOPICS["awattar"], ["ts"], source="awattar", producer=producer) if len(new) else 0
            seen_prices.update(zip(new["ts"], new["price_eur_mwh"]))

            fc = openmeteo.to_dataframe(openmeteo.fetch("forecast"))
            n_f = publish(fc, TOPICS["openmeteo_forecast"], ["location", "ts"], source="openmeteo_forecast",
                          producer=producer, extra={"issued_at": datetime.now(timezone.utc).isoformat()})
            producer.flush(60)
            print(f"{now:%Y-%m-%d %H:%M} UTC  prices: {n_p} new/changed  forecast: {n_f} rows")
        except Exception as exc:  # keep the loop alive on temporary API/Kafka errors
            print(f"{now:%Y-%m-%d %H:%M} UTC  error: {exc}")
        time.sleep(interval)


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish data-source records to Kafka")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill", help="publish historical data (uses the local raw cache)")
    b.add_argument("source", choices=["awattar", "openmeteo"])
    b.add_argument("--kind", choices=["archive", "historical_forecast"], default="archive")
    b.add_argument("--start", required=True)
    b.add_argument("--end", required=True)
    lv = sub.add_parser("live", help="poll APIs and publish continuously")
    lv.add_argument("--interval", type=int, default=900, help="seconds between polls (default 900)")
    args = ap.parse_args()

    if args.cmd == "backfill":
        if args.source == "awattar":
            backfill_awattar(args.start, args.end)
        else:
            backfill_openmeteo(args.kind, args.start, args.end)
    else:
        live(args.interval)


if __name__ == "__main__":
    main()
