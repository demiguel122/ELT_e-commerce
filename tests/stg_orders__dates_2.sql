SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE estimated_delivery_date < created_date