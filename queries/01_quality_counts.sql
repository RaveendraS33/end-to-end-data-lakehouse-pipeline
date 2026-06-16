SELECT
    status,
    count(*) AS record_count
FROM iceberg.quality.transactions_clean
GROUP BY status
ORDER BY record_count DESC;
