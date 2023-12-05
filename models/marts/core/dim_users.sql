{{
  config(
    materialized='incremental'
  )
}}

WITH dim_users__snapshot AS 
(
    SELECT * 
    FROM {{ ref('dim_users__snapshot') }}
)

SELECT *
FROM dim_users__snapshot
WHERE dbt_valid_to IS NULL

