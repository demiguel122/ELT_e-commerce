{{
  config(
    materialized='ephemeral'
  )
}}

WITH dim_date AS (
    SELECT *
    FROM {{ ref("dim_date") }}
),

dim_time AS (
    SELECT *
    FROM {{ ref("dim_time") }}
),

stg_events_with_timestamp AS (
    SELECT
        session_key,
        TO_TIMESTAMP_NTZ(date || ' ' || time_utc, 'YYYY-MM-DD HH24:MI:SS') AS event_timestamp
    FROM {{ ref("stg_sql_server_dbo__events") }} AS e
    JOIN dim_date AS d
    ON e.created_date_key = d.date_key
    JOIN dim_time AS t
    ON e.created_time_utc_key = t.time_key
),

stg_events_first AS (
    SELECT
        session_key,
        event_timestamp AS event_timestamp1,
        ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY event_timestamp1 ASC) AS wf1
    FROM stg_events_with_timestamp e
    WHERE
        (SELECT COUNT(*) FROM stg_events_with_timestamp WHERE session_key = e.session_key) > 1 -- Exclude sessions with just one event
),

stg_events_last AS (
    SELECT
        e.session_key,
        e.event_timestamp AS event_timestamp2,
        ROW_NUMBER() OVER (PARTITION BY e.session_key ORDER BY e.event_timestamp DESC) AS wf2
    FROM stg_events_with_timestamp e
    WHERE
        (SELECT COUNT(*) FROM stg_events_with_timestamp WHERE session_key = e.session_key) > 1 -- Exclude sessions with just one event
)
SELECT
    session_key,
    event_timestamp1 AS first_event_time_utc,
    event_timestamp2 AS last_event_time_utc,
    TIMEDIFF(minute, first_event_time_utc, last_event_time_utc) AS session_length_minutes
FROM stg_events_first
JOIN stg_events_last
USING(session_key)
WHERE wf1 = 1 AND wf2 = 1