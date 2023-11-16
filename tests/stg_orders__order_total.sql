SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }}
WHERE order_cost + shipping_cost != order_total