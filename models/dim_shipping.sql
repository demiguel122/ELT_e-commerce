{{
  config(
    materialized='table'
  )
}}

WITH dim_shipping AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    )

SELECT
    DISTINCT shipping_service AS shipping_service
FROM dim_shipping