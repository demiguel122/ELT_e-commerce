
SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE 
    status = 'delivered'
    AND tracking_id = 'pending' OR tracking_id = ''