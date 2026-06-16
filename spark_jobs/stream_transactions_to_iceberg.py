import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, when
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
    ).withColumn("processed_at", current_timestamp())


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.quality")

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
    clean_df = quality_df.filter(col("error_reason") == VALID_REASON).drop("error_reason")
    bad_df = quality_df.filter(col("error_reason") != VALID_REASON)

    clean_df.writeStream.format("iceberg").outputMode("append").option(
        "checkpointLocation", "s3a://warehouse/checkpoints/transactions_clean"
    ).toTable("lakehouse.quality.transactions_clean")

    bad_df.writeStream.format("iceberg").outputMode("append").option(
        "checkpointLocation", "s3a://warehouse/checkpoints/transactions_bad"
    ).toTable("lakehouse.quality.transactions_bad")

    logger.info("Spark streaming job started for Kafka topic %s", KAFKA_TOPIC)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
