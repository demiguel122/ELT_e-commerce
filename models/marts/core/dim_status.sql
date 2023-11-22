{{
  config(
    materialized='table'
  )
}}

WITH distinct_statuses AS 
(
    SELECT DISTINCT status_key, status
    FROM {{ ref('stg_sql_server_dbo__orders') }}
)

SELECT
    status_key,
    status
FROM distinct_statuses