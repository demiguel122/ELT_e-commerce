SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE status = 'preparing' AND estimated_delivery_date IS NOT NULL