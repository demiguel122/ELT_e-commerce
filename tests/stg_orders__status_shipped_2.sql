SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE status = 'shipped' AND estimated_delivery_at IS NULL