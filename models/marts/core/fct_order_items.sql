{{
  config(
    materialized='table'
  )
}}

WITH fct_order_items__snapshot AS 
(
    SELECT *
    FROM {{ ref("fct_order_items__snapshot") }}
)

SELECT
    order_item_key,
    order_key,
    created_date_key,
    product_key,
    quantity,
    order_cost_item_usd,
    shipping_cost_item_usd,
    user_key,
    status_key,
    shipping_service_key,
    address_key,
    estimated_delivery_date_key,
    estimated_delivery_time_utc_key,
    delivered_date_key,
    delivered_time_utc_key,
    tracking_id,
    promo_key
FROM fct_order_items__snapshot
WHERE dbt_valid_to IS NULL