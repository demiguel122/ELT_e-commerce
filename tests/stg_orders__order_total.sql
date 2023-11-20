SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }}
WHERE order_cost_usd + shipping_cost_usd != order_total_usd