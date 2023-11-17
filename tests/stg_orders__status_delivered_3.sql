SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE status = 'delivered' AND delivered_at IS NULL