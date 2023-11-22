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
        to_date(created_at) AS created_date,
        to_time(created_at) AS created_time_utc,
        to_date(updated_at) AS updated_date,
        to_time(updated_at) AS updated_time_utc,
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
         {{ dbt_utils.generate_surrogate_key(['created_date']) }} AS created_date_key,
         {{ dbt_utils.generate_surrogate_key(['created_time_utc']) }} AS created_time_utc_key,
         {{ dbt_utils.generate_surrogate_key(['updated_date']) }} AS updated_date_key,
         {{ dbt_utils.generate_surrogate_key(['updated_time_utc']) }} AS updated_time_utc_key,
         date_loaded
    FROM src_users
    )

SELECT * FROM stg_users