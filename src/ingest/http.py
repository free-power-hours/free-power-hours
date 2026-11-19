"""Shared HTTP helper: one session, retries with back-off, timeouts."""
from __future__ import annotations

import time

import requests

from .config import USER_AGENT

_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})


def get_json(url: str, params: dict | None = None, retries: int = 4, timeout: int = 120):
    """GET a URL and return the parsed JSON.

    Retries on network errors and 5xx with short exponential back-off.
    On HTTP 429 (rate limit, e.g. Open-Meteo's per-minute limit) it waits 65 s.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = _session.get(url, params=params, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == retries:
                raise
            wait = 2 ** attempt
            print(f"  network error ({exc}); retry {attempt}/{retries - 1} in {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == retries:
                resp.raise_for_status()
            wait = 65 if resp.status_code == 429 else 2 ** attempt
            print(f"  HTTP {resp.status_code} ({resp.text[:120]}); retry {attempt}/{retries - 1} in {wait}s")
            time.sleep(wait)
            continue

        resp.raise_for_status()  # other 4xx: a bug in our request -> fail immediately
        return resp.json()
