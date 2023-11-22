{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS 
(
    SELECT *
    FROM {{ ref("stg_sql_server_dbo__events") }}
)

SELECT *
FROM stg_events