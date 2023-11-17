SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE status = 'shipped' AND shipping_service = ''