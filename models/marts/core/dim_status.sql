{{
  config(
    materialized='table'
  )
}}

WITH distinct_statuses AS 
(
    SELECT DISTINCT status 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_key,
    status
FROM distinct_statuses