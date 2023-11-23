{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email,
        phone_number,
        address_id,
        to_date(created_at) AS created_date_utc,
        to_time(created_at) AS created_time,
        to_date(updated_at) AS updated_date_utc,
        to_time(updated_at) AS updated_time,
        _fivetran_synced AS date_loaded
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
    SELECT
         {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
         first_name,
         last_name,
         email,
         phone_number,
         {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
         created_date_utc,
         created_time,
         updated_date_utc,
         updated_time,
         date_loaded
    FROM src_users
    )

SELECT * FROM stg_users