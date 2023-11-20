{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
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
    FROM src_users
    )

SELECT * FROM stg_users