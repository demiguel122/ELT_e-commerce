SELECT *
FROM {{ ref('stg_sql_server_dbo__orders') }} 
WHERE 
    status = 'delivered' AND delivered_date IS NULL 
    OR status = 'delivered' AND delivered_time_utc IS NULL