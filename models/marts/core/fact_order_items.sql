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
        order_id,
        user_id,
        order_total_usd,
        shipping_cost_usd
    FROM {{ ref("stg_sql_server_dbo__orders") }}
),

stg_products AS 
(
    SELECT 
        product_id,
        price_usd
    FROM {{ ref("stg_sql_server_dbo__products") }}
),

order_items_allocations AS (
    SELECT
        order_id,
        product_id,
        quantity,
        price_usd AS order_cost_item_usd,
        (price_usd / order_total_usd) * shipping_cost_usd AS shipping_cost_item_usd,
        user_id,
        date_loaded
    FROM stg_order_items
    JOIN stg_orders
    USING(order_id)
    JOIN stg_products
    USING(product_id)
)
   
SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} AS order_item_key,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_id,
    quantity,
    order_cost_item_usd::DECIMAL(7,2) AS order_cost_item_usd,
    shipping_cost_item_usd::DECIMAL(7,2) AS shipping_cost_item_usd,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
    date_loaded
FROM order_items_allocations