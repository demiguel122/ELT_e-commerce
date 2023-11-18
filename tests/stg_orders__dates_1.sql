SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE delivered_date < created_date