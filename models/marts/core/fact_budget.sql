{{
  config(
    materialized='table'
  )
}}

WITH stg_budget AS 
(
    SELECT *
    FROM {{ ref('stg_google_sheets__budget') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id', 'date']) }} AS budget_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['date']) }} AS date_key,
    quantity,
    date_loaded
FROM stg_budget