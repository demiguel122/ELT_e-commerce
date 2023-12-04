{{
  config(
    materialized='ephemeral'
  )
}}

WITH stg_events AS (
    SELECT
        session_key,
        event_type
    FROM {{ ref("stg_sql_server_dbo__events") }}
)

SELECT
    session_key,
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,
    SUM(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) AS checkout,
    SUM(CASE WHEN event_type = 'package_shipped' THEN 1 ELSE 0 END) AS package_shipped
FROM stg_events
GROUP BY session_key