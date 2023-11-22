{{
  config(
    materialized='table'
  )
}}

WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

SELECT
    order_key,
    user_key,
    created_date_key,
    created_time_utc_key,
    status_key,
    shipping_service_key,
    address_key,
    estimated_delivery_date_key,
    estimated_delivery_time_utc_key,
    delivered_date_key,
    delivered_time_utc_key,
    tracking_id,
    promo_key,
    date_loaded
FROM stg_orders