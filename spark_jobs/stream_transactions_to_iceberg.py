import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, to_timestamp, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.quality.rules import (
    ERROR_INVALID_AMOUNT,
    ERROR_INVALID_EMAIL,
    ERROR_MISSING_EVENT_TIME,
    ERROR_MISSING_TRANSACTION_ID,
    ERROR_MISSING_USER_ID,
    VALID_REASON,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_transactions")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_time", StringType(), True),
    ]
)

CLEAN_TABLE = "lakehouse.quality.transactions_clean"
BAD_TABLE = "lakehouse.quality.transactions_bad"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("LakehouseKafkaToIceberg")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", ICEBERG_REST_URI)
        .config("spark.sql.catalog.lakehouse.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def add_quality_columns(records_df):
    return records_df.withColumn(
        "error_reason",
        when(col("transaction_id").isNull(), lit(ERROR_MISSING_TRANSACTION_ID))
        .when(col("user_id").isNull(), lit(ERROR_MISSING_USER_ID))
        .when(col("email").isNull() | ~col("email").contains("@"), lit(ERROR_INVALID_EMAIL))
        .when(col("amount").isNull() | (col("amount") <= 0), lit(ERROR_INVALID_AMOUNT))
        .when(col("event_time").isNull(), lit(ERROR_MISSING_EVENT_TIME))
        .otherwise(lit(VALID_REASON)),
    ).withColumn(
        # Parsed event timestamp used for partition pruning on the clean table.
        # Invalid/missing event_time yields null, which is fine: such rows are
        # routed to the bad table (partitioned by processed_at instead).
        "event_ts",
        to_timestamp(col("event_time")),
    ).withColumn("processed_at", current_timestamp())


def create_tables(spark: SparkSession):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.quality")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CLEAN_TABLE} (
            transaction_id STRING,
            user_id INT,
            email STRING,
            amount DOUBLE,
            currency STRING,
            status STRING,
            event_time STRING,
            event_ts TIMESTAMP,
            kafka_timestamp TIMESTAMP,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BAD_TABLE} (
            transaction_id STRING,
            user_id INT,
            email STRING,
            amount DOUBLE,
            currency STRING,
            status STRING,
            event_time STRING,
            event_ts TIMESTAMP,
            kafka_timestamp TIMESTAMP,
            error_reason STRING,
            processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(processed_at))
        """
    )


def write_quality_batch(batch_df, batch_id: int):
    logger.info("Writing quality batch %s", batch_id)

    batch_df.persist()
    clean_batch = batch_df.filter(col("error_reason") == VALID_REASON).drop("error_reason")
    bad_batch = batch_df.filter(col("error_reason") != VALID_REASON)

    clean_count = clean_batch.count()
    bad_count = bad_batch.count()
    logger.info("Batch %s has %s clean records and %s bad records", batch_id, clean_count, bad_count)

    if clean_count > 0:
        clean_batch.writeTo(CLEAN_TABLE).append()

    if bad_count > 0:
        bad_batch.writeTo(BAD_TABLE).append()

    batch_df.unpersist()


def main():
    spark = build_spark()
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
