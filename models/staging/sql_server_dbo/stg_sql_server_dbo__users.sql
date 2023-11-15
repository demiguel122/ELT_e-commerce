{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
         user_id,
         first_name,
         last_name,
         email,
         phone_number,
         total_orders,
         address_id,
         created_at,
         updated_at,
         _fivetran_synced AS date_loaded
    FROM src_users
    )

SELECT * FROM renamed_casted