{{
  config(
    materialized='view'
  )
}}

WITH src_budget AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    ),

stg_budget AS (
    SELECT 
        product_id,
        quantity::INT AS quantity,
        month AS date,
        _fivetran_synced AS date_loaded
    FROM src_budget
    )

SELECT * FROM stg_budget