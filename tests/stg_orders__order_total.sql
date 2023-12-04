SELECT
    order_cost_usd,
    shipping_cost_usd,
    order_total_usd,
    discount_usd
FROM {{ ref('stg_sql_server_dbo__orders') }}
JOIN {{ ref('stg_sql_server_dbo__promos') }}
USING(promo_key)
WHERE (order_cost_usd + shipping_cost_usd) - discount_usd != order_total_usd