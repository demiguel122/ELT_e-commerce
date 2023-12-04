{% snapshot fct_order_items__snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='order_item_key',
      strategy='timestamp',
      updated_at='date_loaded',
    )
}}

WITH stg_order_items AS 
(
    SELECT *
    FROM {{ ref("stg_sql_server_dbo__order_items") }}
),

stg_orders AS 
(
    SELECT *
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
        order_item_key,
        order_key,
        user_key,
        created_date_key,
        product_key,
        quantity,
        price_usd AS order_cost_item_usd,
        (price_usd / order_total_usd) * shipping_cost_usd AS shipping_cost_item_usd,
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
    FROM stg_order_items
    JOIN stg_orders
    USING(order_key)
    JOIN stg_products
    USING(product_key)
)
   
SELECT
    order_item_key,
    order_key,
    created_date_key,
    product_key,
    quantity,
    order_cost_item_usd::DECIMAL(7,2) AS order_cost_item_usd,
    shipping_cost_item_usd::DECIMAL(7,2) AS shipping_cost_item_usd,
    user_key,
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
FROM order_items_allocations

{% endsnapshot %}