{{
  config(
    materialized='table'
  )
}}

WITH fact_events AS (
    SELECT *
    FROM {{ ref("fact_events") }}
),

dim_users AS (
    SELECT
        user_key,
        first_name,
        email
    FROM {{ ref("dim_users") }}
)

SELECT
    session_key,
    user_key,
    first_name,
    email,
    AS first_event_time_utc,
    AS last_event_time_utc,
    AS session_length_minutes,
    AS page_view,
    AS add_to_cart,
    AS checkout,
    AS package_shipped
FROM fact_events
JOIN dim_users
USING(user_key)

-------------------------------

WITH cte1 AS (
    SELECT
    session_id,
    created_time_utc_key,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_time_utc_key) AS wf
FROM {{ ref("fact_events") }}
)

SELECT
    session_id,
    time_utc,
    wf
FROM cte1
JOIN {{ ref("dim_time") }} AS t
ON cte1.created_time_utc_key = t.time_key
WHERE wf = 1

--------------------------

WITH cte2 AS (
    SELECT
        session_id,
        created_time_utc_key,
        RANK() OVER (PARTITION BY session_id ORDER BY created_time_utc_key DESC) AS wf_rank
    FROM {{ ref("fact_events") }}
)

SELECT
    session_id,
    time_utc,
    wf_rank
FROM cte2
JOIN {{ ref("dim_time") }} AS t
    ON cte2.created_time_utc_key = t.time_key
WHERE wf_rank = 1

-------------------------

SELECT
    TIMEDIFF
FROM cte1
JOIN cte2
USING
