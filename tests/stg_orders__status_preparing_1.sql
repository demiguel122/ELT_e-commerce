SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE status = 'preparing' AND shipping_service != 'pending'