{{
  config(
    materialized='view'
  )
}}

WITH src_weather_data AS (
    SELECT *
    FROM {{ source('meteostat', 'weather_data') }}
)

SELECT *
FROM src_weather_data