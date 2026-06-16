from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="lakehouse_pipeline_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "kafka", "spark", "iceberg"],
) as dag:
    publish_sample_event = BashOperator(
        task_id="publish_sample_event",
        bash_command="curl -X POST http://api:8000/transactions/sample",
    )

    explain_next_steps = BashOperator(
        task_id="explain_next_steps",
        bash_command="echo 'Spark streaming consumes Kafka continuously and writes Iceberg tables to MinIO.'",
    )

    publish_sample_event >> explain_next_steps
