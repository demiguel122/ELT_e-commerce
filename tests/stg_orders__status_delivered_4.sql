WITH  cte AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__orders') }} 
    WHERE status = 'delivered'
)

SELECT *
FROM cte 
WHERE tracking_id = 'pending' OR tracking_id = ''