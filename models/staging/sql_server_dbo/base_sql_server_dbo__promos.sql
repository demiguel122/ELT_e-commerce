{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
         promo_id,
         discount,
         status,
         _fivetran_synced AS date_loaded
    FROM src_promos
    )

SELECT * FROM renamed_casted