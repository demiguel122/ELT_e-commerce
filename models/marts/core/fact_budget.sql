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

SELECT *
FROM stg_budget