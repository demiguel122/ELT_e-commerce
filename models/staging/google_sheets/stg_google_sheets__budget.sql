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
        {{ dbt_utils.generate_surrogate_key(['product_id', 'month']) }} AS budget_key,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
        {{ dbt_utils.generate_surrogate_key(['month']) }} AS date_key,
        quantity::INT as quantity,
        _fivetran_synced as date_loaded 
    FROM src_budget
    )

SELECT * FROM stg_budget