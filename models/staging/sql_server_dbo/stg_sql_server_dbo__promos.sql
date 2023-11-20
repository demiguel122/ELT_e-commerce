{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

stg_promos AS (
    SELECT
        decode
            (promo_id,
            'task-force', 'task-force',
            'instruction set', 'instruction set',
            'leverage', 'leverage',
            'Optional', 'optional',
            'Mandatory', 'mandatory',
            'Digitized', 'digitized') AS promo_id,
         discount AS discount_usd,
         status,
         _fivetran_synced AS date_loaded
    FROM src_promos
    )

SELECT * FROM stg_promos