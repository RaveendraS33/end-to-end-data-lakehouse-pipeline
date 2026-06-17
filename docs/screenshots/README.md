# Screenshots

Proof captures from a live run of the stack, embedded in the project README's
Proof section:

| File | Shows |
|------|-------|
| `trino-query.png` | Trino query results: rejection-reason breakdown and a clean-table sample with the partition day |
| `minio-bucket.png` | MinIO console: the `warehouse/quality/` prefix holding the `transactions_clean` and `transactions_bad` Iceberg data |
| `airflow-dag.png` | The `transactions_backfill` DAG run succeeded in the Airflow UI |
| `streamlit-dashboard.png` | The Streamlit dashboard with live metrics and charts |

## Regenerate

With the stack running, capture all four with:

```powershell
pip install playwright
python scripts/capture_screenshots.py "<airflow-admin-password>"
```

The Airflow admin password is printed at startup and stored in the container at
`/opt/airflow/standalone_admin_password.txt`.
