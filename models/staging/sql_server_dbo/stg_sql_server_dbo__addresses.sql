{{
  config(
    materialized='view'
  )
}}

WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

renamed_casted AS (
    SELECT
         address_id,
         address,
         zipcode,
         state,
         country,
         _fivetran_synced AS date_loaded
    FROM src_addresses
    )

SELECT * FROM renamed_casted