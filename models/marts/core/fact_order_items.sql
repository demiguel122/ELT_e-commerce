{{
  config(
    materialized='table'
  )
}}

WITH stg_order_items AS 
(
    SELECT *
    FROM {{ ref("stg_sql_server_dbo__order_items") }}
),

stg_orders AS 
(
    SELECT
        order_key,
        user_key,
        order_total_usd,
        shipping_cost_usd
    FROM {{ ref("stg_sql_server_dbo__orders") }}
),

stg_products AS 
(
    SELECT 
        product_key,
        price_usd
    FROM {{ ref("stg_sql_server_dbo__products") }}
),

order_items_allocations AS (
    SELECT
        order_key,
        product_key,
        quantity,
        price_usd AS order_cost_item_usd,
        (price_usd / order_total_usd) * shipping_cost_usd AS shipping_cost_item_usd,
        user_key,
        date_loaded
    FROM stg_order_items
    JOIN stg_orders
    USING(order_key)
    JOIN stg_products
    USING(product_key)
)
   
SELECT
    order_item_key,
    order_key,
    product_key,
    quantity,
    order_cost_item_usd::DECIMAL(7,2) AS order_cost_item_usd,
    shipping_cost_item_usd::DECIMAL(7,2) AS shipping_cost_item_usd,
    user_key,
    date_loaded
FROM order_items_allocations