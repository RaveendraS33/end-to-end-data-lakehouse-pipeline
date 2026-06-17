# Verification

This pipeline has been run end-to-end on Docker Desktop. The outputs below are
real and reproducible with the commands shown. Screenshots of the service UIs
live in [screenshots/](screenshots/).

## 1. Streaming ingestion with quality routing

Events published to the API flow through Kafka -> Spark -> Iceberg and split
into clean and bad tables. After publishing samples and rule violations:

```sql
SELECT error_reason, count(*) FROM iceberg.quality.transactions_bad GROUP BY error_reason ORDER BY 1;
```

```
invalid_amount       3
invalid_currency     1
invalid_email        6
invalid_event_time   1
invalid_status       1
missing_user_id      3
```

All eight quality rules are exercised, including the stricter ones added later
(`invalid_currency`, `invalid_status`, `invalid_event_time`).

## 2. Partitioning is materialized

```sql
SHOW CREATE TABLE iceberg.quality.transactions_clean;
-- ... partitioning = ARRAY['day(event_ts)']

SELECT count(*) AS partition_count FROM iceberg.quality."transactions_clean$partitions";
-- partition_count = 6
```

The clean table is physically partitioned by event day; the metadata table
confirms multiple day partitions exist.

## 3. Idempotent upserts (MERGE)

Running the batch backfill twice does not duplicate rows:

```sql
SELECT count(*) - count(DISTINCT transaction_id) AS duplicate_rows
FROM iceberg.quality.transactions_clean;
-- duplicate_rows = 0
```

## 4. Batch backfill

```
docker compose run --rm spark-backfill
# INFO Backfill complete: 5 clean, 3 bad
```

Re-running keeps the clean `bf-%` count at 5 (MERGE upsert), proving the batch
path is idempotent.

## 5. Integration tests (against the live stack)

```
RUN_INTEGRATION_TESTS=1 python -m pytest tests -m integration -q
# 3 passed
```

Covers clean routing, bad routing, and MERGE idempotency end-to-end.

## 6. Airflow orchestration

The `transactions_backfill` DAG launches the Spark backfill via DockerOperator:

```
airflow dags list-runs --dag-id transactions_backfill
# run_id=manual_verify_... state=success
```

## Reproduce

```powershell
docker compose up --build -d kafka minio minio-init iceberg-postgres iceberg-rest trino api spark
# publish a few events
curl -X POST http://localhost:8000/transactions/sample
curl -X POST http://localhost:8000/transactions/sample-bad
# query in Trino
docker exec -it lakehouse-trino trino
# run the integration suite
$env:RUN_INTEGRATION_TESTS = "1"; python -m pytest tests -m integration
# dashboard + orchestration
docker compose --profile tools up --build -d dashboard          # http://localhost:8501
docker compose --profile orchestration up -d airflow            # http://localhost:8088
```
