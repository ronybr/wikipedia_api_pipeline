-- Description: This model aggregates the recent changes data to 15-minute slots and returns the slot with the most changes.

{{ config(materialized='table') }}

-- Cast the timestamp to a timestamp type
WITH parsed_timestamps AS (
    SELECT
        STRPTIME(timestamp, '%Y-%m-%dT%H:%M:%SZ') AS change_time
    FROM
        -- {{ source('english_wikipedia', 'recent_changes') }}
        english_wikipedia.recent_changes
),
-- Create 15-minute time slots
time_slots AS (
    SELECT
        change_time,
        -- Calculate the start of the 15-minute aligned slot
        DATE_TRUNC('minute', change_time) - INTERVAL (EXTRACT(MINUTE FROM change_time) % 15) MINUTE AS time_slot_start,
        -- Add 30 minutes to get the end of the slot
        DATE_TRUNC('minute', change_time) - INTERVAL (EXTRACT(MINUTE FROM change_time) % 15) MINUTE + INTERVAL 30 MINUTE AS time_slot_end
    FROM
        parsed_timestamps
),
-- Count the number of changes in each slot
slot_changes AS (
    SELECT
        time_slot_start,
        time_slot_end,
        COUNT(*) AS num_changes
    FROM
        time_slots
    GROUP BY
        time_slot_start, time_slot_end
),
-- Rank the slots by the number of changes
ranked_slots AS (
    SELECT
        time_slot_start,
        time_slot_end,
        num_changes,
        RANK() OVER (ORDER BY num_changes DESC) AS rank
    FROM
        slot_changes
)
SELECT
    time_slot_start,
    time_slot_end,
    num_changes
FROM
    ranked_slots
WHERE
    -- Return the top 3 slots with the most changes
    rank <= 3;