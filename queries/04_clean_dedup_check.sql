-- Idempotency check: because clean records are upserted via Iceberg MERGE
-- on transaction_id, total rows should always equal distinct transaction_ids,
-- even after replaying the stream or re-running the backfill. duplicate_rows = 0.
SELECT
    count(*)                          AS total_rows,
    count(DISTINCT transaction_id)    AS distinct_ids,
    count(*) - count(DISTINCT transaction_id) AS duplicate_rows
FROM iceberg.quality.transactions_clean;
