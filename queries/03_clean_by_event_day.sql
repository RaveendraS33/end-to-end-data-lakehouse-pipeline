-- Partition-pruned scan of the clean table.
-- Filtering on event_ts lets Iceberg skip every day partition outside the range.
SELECT
    date(event_ts) AS event_day,
    count(*)        AS record_count,
    round(sum(amount), 2) AS total_amount
FROM iceberg.quality.transactions_clean
WHERE event_ts >= TIMESTAMP '2026-06-01 00:00:00'
  AND event_ts <  TIMESTAMP '2026-07-01 00:00:00'
GROUP BY date(event_ts)
ORDER BY event_day;
