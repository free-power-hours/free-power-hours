"""Live scoring: weather forecasts from Kafka -> negative-price probabilities back to Kafka.

Reads `openmeteo.forecast` with Spark Structured Streaming, builds features with the
same function as training (src/features.py), applies the saved model and writes one
message per hour to `predictions.cheap_hours`.

Run from the project root (needs the trained model in models/cheap_hours):
    python -m src.predict                     # keep running, score each new forecast poll
    python -m src.predict --once              # score what is in the topic now, then exit
    python -m src.predict --from-beginning    # also re-score forecasts already in the topic

Feed it with the live producer in another terminal:
    python -m src.ingest.stream live --interval 900
"""
from __future__ import annotations

import argparse

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, functions as F

from .features import LOCS, WEATHER_SCHEMA, parse_kafka, weather_features
from .ingest.config import KAFKA_BOOTSTRAP as BOOT, PROJECT_ROOT, TOPICS

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
OUTPUT_TOPIC = "predictions.cheap_hours"
MODEL_DIR = PROJECT_ROOT / "models" / "cheap_hours"
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints" / "predict"
ISO = "yyyy-MM-dd'T'HH:mm:ssxxx"   # session time zone is UTC -> 2026-11-20T13:00:00+00:00, as in the data contract


def make_scorer(model: PipelineModel, threshold: float):
    """foreachBatch function: score one micro-batch of forecast rows."""

    def score(batch: DataFrame, batch_id: int) -> None:
        # Only hours for which all locations arrived; a poll split across two
        # micro-batches would otherwise be scored with missing locations (= 0).
        complete = (batch.groupBy("ts").agg(F.countDistinct("location").alias("n"))
                         .filter(F.col("n") == len(LOCS)).select("ts"))
        wx = batch.join(complete, "ts")
        if wx.isEmpty():
            return
        # Format timestamps inside Spark (UTC session) so the result is the same on any machine
        issued_at = wx.agg(F.date_format(F.max(F.coalesce("issued_at", "ingested_at")), ISO)).first()[0]

        preds = (model.transform(weather_features(wx))
                 .withColumn("p_negative", F.round(vector_to_array("probability")[1], 4))
                 .withColumn("is_negative", F.col("p_negative") >= threshold)
                 .select("ts", "p_negative", "is_negative")
                 .cache())

        (preds.select(F.date_format("ts", ISO).alias("key"),
                      F.to_json(F.struct(
                          F.date_format("ts", ISO).alias("ts"),
                          "p_negative", "is_negative",
                          F.lit(threshold).alias("threshold"),
                          F.lit(MODEL_DIR.name).alias("model"),
                          F.lit(issued_at).alias("forecast_issued_at"),
                          F.date_format(F.current_timestamp(), ISO).alias("scored_at"),
                      )).alias("value"))
              .write.format("kafka").option("kafka.bootstrap.servers", BOOT)
              .option("topic", OUTPUT_TOPIC).save())

        _print_summary(preds, batch_id, issued_at, threshold)
        preds.unpersist()

    return score


def _print_summary(preds: DataFrame, batch_id: int, issued_at: str, threshold: float) -> None:
    local = F.from_utc_timestamp("ts", "Europe/Vienna")
    rows = (preds.filter(F.col("ts") >= F.date_trunc("hour", F.current_timestamp()))
                 .withColumn("local", F.date_format(local, "EEE dd.MM. HH:00"))
                 .orderBy("ts").collect())
    hits = [r for r in rows if r.is_negative]
    print(f"\n[batch {batch_id}] forecast issued {issued_at}: "
          f"{len(rows)} upcoming hours scored, {len(hits)} with p >= {threshold} (Vienna time)")
    for r in hits:
        print(f"   {r.local}  p_negative = {r.p_negative:.2f}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Score live weather forecasts with the trained model")
    ap.add_argument("--threshold", type=float, default=0.3,
                    help="probability from which an hour counts as negative (default 0.3)")
    ap.add_argument("--once", action="store_true", help="process what is in the topic now, then exit")
    ap.add_argument("--from-beginning", action="store_true",
                    help="start at offset 0 instead of new messages (only for a fresh checkpoint)")
    ap.add_argument("--trigger", default="30 seconds", help="micro-batch interval (default 30 seconds)")
    args = ap.parse_args()

    if not MODEL_DIR.exists():
        raise SystemExit(f"No model at {MODEL_DIR}. Run notebooks/training.ipynb first.")

    spark = (SparkSession.builder.appName("predict-cheap-hours")
             .config("spark.jars.packages", KAFKA_PACKAGE)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8")   # tiny batches: fewer tasks = faster
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    model = PipelineModel.load(str(MODEL_DIR))

    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("subscribe", TOPICS["openmeteo_forecast"])
           .option("startingOffsets", "earliest" if args.from_beginning else "latest")
           .load())
    writer = (parse_kafka(raw, WEATHER_SCHEMA).writeStream
              .foreachBatch(make_scorer(model, args.threshold))
              .option("checkpointLocation", str(CHECKPOINT_DIR)))
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime=args.trigger)

    print(f"Scoring {TOPICS['openmeteo_forecast']} -> {OUTPUT_TOPIC} (Kafka {BOOT}, threshold {args.threshold})")
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
