{{
  config(
    materialized='table'
  )
}}

WITH stg_users AS 
(
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__users') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
    first_name,
    last_name,
    email,
    phone_number,
    address_id,
    created_date,
    created_time_utc,
    updated_date,
    updated_time_utc
FROM stg_users