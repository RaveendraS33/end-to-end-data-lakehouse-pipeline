"""Airflow DAG: orchestrate the batch backfill of historical transactions.

The DAG launches the Spark backfill job as a sibling container via
DockerOperator (talking to the host Docker daemon through the mounted socket).
The Spark image `lakehouse-spark:local` already contains the job code and the
seed data, so no bind mounts are needed -- the container only has to join the
shared `lakehouse-net` network to reach the Iceberg REST catalog and MinIO.

Prerequisites (configured in docker-compose.yml):
- the Spark image is tagged `lakehouse-spark:local`
- the Airflow container has /var/run/docker.sock mounted
- `apache-airflow-providers-docker` is installed (via _PIP_ADDITIONAL_REQUIREMENTS)
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


with DAG(
    dag_id="transactions_backfill",
    description="Replay historical transactions into the Iceberg lakehouse tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "spark", "iceberg", "backfill"],
) as dag:
    run_backfill = DockerOperator(
        task_id="run_spark_backfill",
        image="lakehouse-spark:local",
        container_name="lakehouse-spark-backfill-airflow",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="lakehouse-net",
        mount_tmp_dir=False,
        environment={
            "AWS_ACCESS_KEY_ID": MINIO_USER,
            "AWS_SECRET_ACCESS_KEY": MINIO_PASSWORD,
            "MINIO_ROOT_USER": MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
            "AWS_REGION": AWS_REGION,
            "BACKFILL_PATH": "/app/seed/backfill_sample.jsonl",
        },
        command=[
            "/opt/spark/bin/spark-submit",
            "--packages",
            SPARK_PACKAGES,
            "spark_jobs/backfill_transactions.py",
        ],
    )
