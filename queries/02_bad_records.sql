SELECT
    error_reason,
    count(*) AS bad_record_count
FROM iceberg.quality.transactions_bad
GROUP BY error_reason
ORDER BY bad_record_count DESC;
