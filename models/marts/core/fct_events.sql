{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS 
(
    SELECT *
    FROM {{ ref("stg_sql_server_dbo__events") }}
)

SELECT
    event_key,
    event_type_key,
    user_key,
    session_key,
    order_key,
    product_key,
    created_date_key,
    created_time_utc_key
FROM stg_events