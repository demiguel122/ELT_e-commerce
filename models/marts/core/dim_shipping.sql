{{
  config(
    materialized='table'
  )
}}

WITH distinct_services AS (
    SELECT DISTINCT shipping_service 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} AS shipping_service_key,
    shipping_service
FROM distinct_services