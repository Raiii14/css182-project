from datetime import datetime, timezone
from itertools import chain
from pathlib import Path

from cassandra.cluster import Cluster
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from model import MAGNITUDE_SPLIT, MagnitudeBand


MESSAGE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("magnitude", DoubleType()),
        StructField("place", StringType()),
        StructField("occurred_at_ms", LongType()),
        StructField("source_updated_at_ms", LongType()),
        StructField("captured_at_ms", LongType()),
        StructField("details_url", StringType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("depth_km", DoubleType()),
    ]
)

INSERT = """
INSERT INTO finalproject.earthquakes_by_day (
    event_day, event_id, magnitude, place, occurred_at, source_updated_at,
    captured_at, details_url, longitude, latitude, depth_km, source_topic
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?
"""


def write_batch(batch: DataFrame, batch_id: int) -> None:
    rows = iter(batch.toLocalIterator())
    first = next(rows, None)
    if first is None:
        return

    cluster = Cluster(["127.0.0.1"], port=9042)
    count = 0
    try:
        session = cluster.connect("finalproject")
        insert = session.prepare(INSERT)
        for row in chain((first,), rows):
            session.execute(
                insert,
                (
                    row.event_day,
                    row.event_id,
                    row.magnitude,
                    row.place,
                    datetime.fromtimestamp(row.occurred_at_ms / 1000, tz=timezone.utc),
                    datetime.fromtimestamp(row.source_updated_at_ms / 1000, tz=timezone.utc),
                    datetime.fromtimestamp(row.captured_at_ms / 1000, tz=timezone.utc),
                    row.details_url,
                    row.longitude,
                    row.latitude,
                    row.depth_km,
                    row.source_topic,
                    row.captured_at_ms * 1000,
                ),
            )
            count += 1
    finally:
        cluster.shutdown()
    print(f"Spark batch={batch_id} stored={count} rows in Cassandra", flush=True)


def main() -> None:
    spark = (
        SparkSession.builder.appName("USGS Earthquake Pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    topics = ",".join(band.value for band in MagnitudeBand)
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .load()
    )
    records = kafka.select(
        F.col("topic").cast("string").alias("source_topic"),
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("message"),
    ).select("source_topic", "message.*")
    records = records.where(
        F.col("event_id").isNotNull()
        & F.col("magnitude").isNotNull()
        & F.col("place").isNotNull()
        & F.col("occurred_at_ms").isNotNull()
        & F.col("source_updated_at_ms").isNotNull()
        & F.col("captured_at_ms").isNotNull()
        & F.col("details_url").isNotNull()
        & (
            ((F.col("source_topic") == MagnitudeBand.LOW.value) & (F.col("magnitude") < MAGNITUDE_SPLIT))
            | ((F.col("source_topic") == MagnitudeBand.HIGH.value) & (F.col("magnitude") >= MAGNITUDE_SPLIT))
        )
    )
    records = (
        records.withColumn("occurred_at", F.expr("timestamp_millis(occurred_at_ms)"))
        .withColumn("source_updated_at", F.expr("timestamp_millis(source_updated_at_ms)"))
        .withColumn("captured_at", F.expr("timestamp_millis(captured_at_ms)"))
        .withColumn("event_day", F.to_date("occurred_at"))
    )
    checkpoint = Path(__file__).resolve().parent / ".runtime" / "spark-checkpoint"
    query = (
        records.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", str(checkpoint))
        .trigger(processingTime="15 seconds")
        .start()
    )
    print(f"Spark streaming {topics}; UI: http://localhost:4040", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
