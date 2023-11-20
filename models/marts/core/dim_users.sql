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
    {{ dbt_utils.generate_surrogate_key(['created_date']) }} AS created_date_key,
    {{ dbt_utils.generate_surrogate_key(['created_time_utc']) }} AS created_time_utc_key,
    {{ dbt_utils.generate_surrogate_key(['updated_date']) }} AS updated_date_key,
    {{ dbt_utils.generate_surrogate_key(['updated_time_utc']) }} AS updated_time_utc_key
FROM stg_users