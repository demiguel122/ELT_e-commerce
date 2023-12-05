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

SELECT
    user_key,
    first_name,
    last_name,
    email,
    phone_number,
    address_key,
    created_date,
    created_time_utc,
    updated_date,
    updated_time_utc
FROM dim_users__snapshot
WHERE dbt_valid_to IS NULL

