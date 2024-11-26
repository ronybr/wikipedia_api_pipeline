-- models/recent_changes_aggregated.sql
WITH timestamped_changes AS (
    SELECT
        change_id,
        timestamp,
        -- Round the timestamp down to the nearest 15-minute interval
        date_trunc('minute', timestamp) - INTERVAL '1 minute' * (EXTRACT(minute FROM timestamp) % 15) AS time_slot
    FROM {{ ref('recent_changes') }}  -- Reference to the existing recent_changes table
)
SELECT
    time_slot,
    COUNT(*) AS num_changes
FROM timestamped_changes
GROUP BY time_slot
ORDER BY time_slot;
