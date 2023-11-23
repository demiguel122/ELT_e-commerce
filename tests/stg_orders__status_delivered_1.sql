SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE status = 'delivered' AND shipping_service = ''