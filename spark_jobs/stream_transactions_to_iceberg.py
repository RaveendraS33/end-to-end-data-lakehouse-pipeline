import logging
import os

from pyspark.sql.functions import col, from_json

from spark_jobs.lakehouse_common import (
    TRANSACTION_SCHEMA,
    add_quality_columns,
    build_spark,
    create_tables,
    write_clean_and_bad,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_transactions")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def write_quality_batch(batch_df, batch_id: int):
    write_clean_and_bad(batch_df, label=f"micro-batch {batch_id}")


def main():
    spark = build_spark("LakehouseKafkaToIceberg")
    spark.sparkContext.setLogLevel("WARN")

    create_tables(spark)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("record"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("record.*", "kafka_timestamp")

    quality_df = add_quality_columns(parsed_df)

    quality_df.writeStream.foreachBatch(write_quality_batch).outputMode("append").option(
        "checkpointLocation", "s3a://warehouse/checkpoints/transactions_quality_v3"
    ).start()

    logger.info("Spark streaming job started for Kafka topic %s", KAFKA_TOPIC)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
