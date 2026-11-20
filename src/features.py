"""Feature engineering shared by training (notebooks/training.ipynb) and live scoring (src/predict.py).

Both must build features with exactly this code: a saved model expects the
same columns, in the same meaning, as it was trained on.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F, types as T

from .ingest.config import LOCATIONS

LOCS = list(LOCATIONS)

# Weather messages (openmeteo.* topics), only the fields the model uses.
# `issued_at` exists only in openmeteo.forecast and stays null elsewhere.
WEATHER_SCHEMA = T.StructType([T.StructField(c, t) for c, t in [
    ("ts", T.TimestampType()), ("location", T.StringType()),
    ("temperature_2m", T.DoubleType()), ("shortwave_radiation", T.DoubleType()),
    ("cloud_cover", T.DoubleType()), ("wind_speed_100m", T.DoubleType()),
    ("ingested_at", T.TimestampType()), ("issued_at", T.TimestampType())]])

PRICE_SCHEMA = T.StructType([T.StructField("ts", T.TimestampType()),
                             T.StructField("price_eur_mwh", T.DoubleType()),
                             T.StructField("ingested_at", T.TimestampType())])

WEATHER_VARS = ["temperature_2m", "shortwave_radiation", "cloud_cover", "wind_speed_100m"]
DE_SOLAR = ["munich_rad", "stuttgart_rad", "berlin_rad"]
DE_WIND = ["husum_wind", "hamburg_wind", "berlin_wind"]
CALENDAR = ["hour", "month"]


def parse_kafka(raw: DataFrame, schema: T.StructType) -> DataFrame:
    """Kafka rows (binary key/value) -> one column per JSON field."""
    return raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")


def weather_features(wx: DataFrame) -> DataFrame:
    """Long weather table (one row per location and hour) -> one feature row per hour.

    * Per location: solar radiation, wind at 100 m, temperature, cloud cover (munich_rad, husum_wind, ...)
    * DE totals: de_solar_sum, de_wind_mean (prices react to total German output)
    * Calendar (Europe/Vienna local time): hour, month, weekend
    """
    # Upsert semantics from the data contract: keep the latest message per key
    latest = wx.groupBy("location", "ts").agg(*[F.max_by(c, "ingested_at").alias(c) for c in WEATHER_VARS])
    wide = latest.groupBy("ts").pivot("location", LOCS).agg(
        F.first("shortwave_radiation").alias("rad"), F.first("wind_speed_100m").alias("wind"),
        F.first("temperature_2m").alias("temp"), F.first("cloud_cover").alias("cloud")).na.fill(0)
    local = F.from_utc_timestamp("ts", "Europe/Vienna")
    return (wide.withColumn("de_solar_sum", sum(F.col(c) for c in DE_SOLAR))
                .withColumn("de_wind_mean", sum(F.col(c) for c in DE_WIND) / len(DE_WIND))
                .withColumn("hour", F.hour(local))
                .withColumn("month", F.month(local))
                .withColumn("weekend", F.dayofweek(local).isin(1, 7).cast("double")))


def numeric_feature_cols(features: DataFrame) -> list[str]:
    """All feature columns except the timestamp and the calendar categories."""
    return [c for c in features.columns if c not in ["ts", *CALENDAR]]
