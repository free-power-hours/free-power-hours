"""Publish data-source records to Kafka.

Kafka is the hand-over point between the Data Source Layer and the
Storage/Processing layers: producers here write one JSON message per
measurement; Spark Structured Streaming consumes the topics downstream.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer

from .config import KAFKA_BOOTSTRAP


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "fph-ingest",
        "enable.idempotence": True,   # no duplicates on producer retries
        "linger.ms": 50,              # small batching window -> fewer, larger requests
        "compression.type": "zstd",
    })


def _json_default(obj):
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    raise TypeError(f"not JSON serializable: {type(obj)}")


def publish(df: pd.DataFrame, topic: str, key_cols: list[str], source: str,
            producer: Producer | None = None, extra: dict | None = None) -> int:
    """Send each DataFrame row as one JSON message. Returns the number of messages sent.

    Every message gets `source` and `ingested_at` (lineage), plus any `extra` fields.
    NaN values become JSON null (NaN is not valid JSON and would break Spark's parser).
    """
    own_producer = producer is None
    producer = producer or make_producer()
    meta = {"source": source, "ingested_at": datetime.now(timezone.utc).isoformat(), **(extra or {})}
    clean = df.astype(object).where(df.notna(), None)

    errors: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    n = 0
    for rec in clean.to_dict(orient="records"):
        key = "|".join(str(rec[c].isoformat() if isinstance(rec[c], pd.Timestamp) else rec[c]) for c in key_cols)
        value = json.dumps({**rec, **meta}, default=_json_default)
        while True:
            try:
                producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:          # local queue full -> let librdkafka send, then retry
                producer.poll(0.5)
        n += 1
        if n % 10_000 == 0:
            producer.poll(0)

    remaining = producer.flush(60) if own_producer else producer.poll(0)
    if own_producer and remaining:
        raise RuntimeError(f"{remaining} messages not delivered to {topic} (is Kafka running at {KAFKA_BOOTSTRAP}?)")
    if errors:
        raise RuntimeError(f"{len(errors)} delivery errors on {topic}, first: {errors[0]}")
    return n
