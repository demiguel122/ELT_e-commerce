SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE status = 'delivered' AND estimated_delivery_date IS NULL