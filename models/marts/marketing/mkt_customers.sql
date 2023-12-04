{{
  config(
    materialized='table'
  )
}}

WITH dim_users AS (
    SELECT
        user_key,
        first_name,
        last_name,
        email,
        phone_number,
        created_date,
        updated_date,
        address_key
    FROM {{ ref("dim_users") }}
),

dim_addresses AS (
    SELECT
        address_key,
        address,
        zipcode,
        state,
        country
    FROM {{ ref("dim_addresses") }}
),

dim_promos AS (
    SELECT
        promo_key,
        discount_usd
    FROM {{ ref("dim_promos") }}
),

fct_order_items AS (
    SELECT
        user_key,
        order_key,
        order_cost_item_usd,
        shipping_cost_item_usd,
        promo_key,
        quantity,
        product_key
    FROM {{ ref("fct_order_items") }}
)

SELECT
    user_key,
    first_name,
    last_name,
    email,
    phone_number,
    created_date,
    updated_date,
    address,
    zipcode,
    state,
    country,
    COUNT(DISTINCT order_key) AS total_no_orders,
    SUM(order_cost_item_usd) AS total_order_cost_usd,
    SUM(shipping_cost_item_usd) AS total_shipping_cost_usd,
    SUM(discount_usd) AS total_discount_usd,
    SUM(quantity) AS total_quantity_product,
    COUNT(DISTINCT product_key) AS total_diff_products
FROM dim_users
JOIN dim_addresses
USING(address_key)
JOIN fct_order_items
USING(user_key)
JOIN dim_promos
USING(promo_key)
GROUP BY
    user_key,
    first_name,
    last_name,
    email,
    phone_number,
    created_date,
    updated_date,
    address,
    zipcode,
    state,
    country