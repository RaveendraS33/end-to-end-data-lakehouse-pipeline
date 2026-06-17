"""Batch backfill: replay historical transaction events from a JSON file.

Reads newline-delimited JSON (one transaction per line) from BACKFILL_PATH,
applies the same quality rules and table routing as the streaming job, and
appends the results into the same partitioned Iceberg tables. This demonstrates
that streaming and batch share a single ingestion code path
(`lakehouse_common`) -- the canonical lakehouse pattern.

Run (one-shot, against a running stack):

    docker compose run --rm spark-backfill
"""
import logging
import os

from pyspark.sql.functions import lit

from spark_jobs.lakehouse_common import (
    TRANSACTION_SCHEMA,
    add_quality_columns,
    build_spark,
    create_tables,
    write_clean_and_bad,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BACKFILL_PATH = os.getenv("BACKFILL_PATH", "/app/seed/backfill_sample.jsonl")


def main():
    spark = build_spark("LakehouseBackfill")
    spark.sparkContext.setLogLevel("WARN")

    create_tables(spark)

    logger.info("Reading backfill events from %s", BACKFILL_PATH)
    records_df = spark.read.schema(TRANSACTION_SCHEMA).json(BACKFILL_PATH)

    # Batch records have no Kafka envelope, so kafka_timestamp is null. The
    # column still exists on the table, so we add it explicitly to align.
    records_df = records_df.withColumn("kafka_timestamp", lit(None).cast("timestamp"))

    quality_df = add_quality_columns(records_df)
    clean_count, bad_count = write_clean_and_bad(quality_df, label="backfill")

    logger.info("Backfill complete: %s clean, %s bad", clean_count, bad_count)
    spark.stop()


if __name__ == "__main__":
    main()
