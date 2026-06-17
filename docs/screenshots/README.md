# Screenshots

Add the following PNGs here and they will render in the project README's Proof
section. Capture them with the stack running:

| File | What to capture |
|------|-----------------|
| `trino-query.png` | A Trino query result, e.g. `SELECT error_reason, count(*) FROM iceberg.quality.transactions_bad GROUP BY error_reason` |
| `minio-bucket.png` | The MinIO console (http://localhost:9001) showing the `warehouse/quality/` Iceberg data files |
| `airflow-dag.png` | The `transactions_backfill` DAG run succeeded (green) in the Airflow UI (http://localhost:8088) |
| `streamlit-dashboard.png` | The Streamlit dashboard (http://localhost:8501) with metrics and charts |
