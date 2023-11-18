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
    total_orders,
    address_id,
    to_date(created_at) AS created_date,
    to_time(created_at) AS created_time,
    to_date(updated_at) AS updated_date,
    to_time(updated_at) AS updated_time,
FROM stg_users