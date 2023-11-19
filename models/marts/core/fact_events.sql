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
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS event_key,
    {{ dbt_utils.generate_surrogate_key(['event_type']) }} AS event_type_key,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
    session_id,
    {{ dbt_utils.generate_surrogate_key(['page_url']) }} AS page_url_key,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['created_date']) }} AS created_date_key,
    {{ dbt_utils.generate_surrogate_key(['created_time']) }} AS created_time_key,
    date_loaded
FROM stg_events