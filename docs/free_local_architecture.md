# Free Local Architecture

This project is intentionally designed to avoid cloud costs.

## Free Replacements

| Cloud-style component | Free local component |
| --- | --- |
| Amazon S3 | MinIO |
| Confluent Cloud or AWS MSK | Kafka container |
| Databricks or EMR | Spark container |
| Athena or managed Trino | Trino container |
| Managed Airflow | Airflow container |

## Recommended Laptop Resources

Minimum:

- 16 GB RAM
- Docker Desktop
- 20 GB free disk space

Better:

- 32 GB RAM
- Docker Desktop with 8+ GB memory allocated
- SSD storage

## Free Development Strategy

Build in stages:

1. Start Kafka, MinIO, and API.
2. Confirm API publishes to Kafka.
3. Add Spark streaming.
4. Write clean and bad records to Iceberg.
5. Query with Trino.
6. Add Airflow last.

This avoids running every heavy service while debugging early code.
