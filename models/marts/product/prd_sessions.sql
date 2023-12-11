{{
  config(
    materialized='table'
  )
}}

WITH fct_events AS (
    SELECT *
    FROM {{ ref("fct_events") }}
),

dim_users AS (
    SELECT
        user_key,
        first_name,
        email
    FROM {{ ref("dim_users") }}
),

int_session_lenghts AS (
    SELECT
        *
    FROM {{ ref("int_session_lengths") }}
),

int_event_types_count AS (
    SELECT
        *
    FROM {{ ref("int_event_types_count") }}
)

SELECT DISTINCT
    session_key,
    user_key,
    first_name,
    email,
    first_event_time_utc,
    last_event_time_utc,
    session_length_minutes,
    page_view,
    add_to_cart,
    checkout,
    package_shipped
FROM fct_events
JOIN dim_users
USING(user_key)
JOIN int_session_lenghts
USING(session_key)
JOIN int_event_types_count
USING (session_key)