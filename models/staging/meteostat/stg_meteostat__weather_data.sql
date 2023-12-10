{{
  config(
    materialized='view'
  )
}}

WITH src_weather_data AS (
    SELECT *
    FROM {{ source('meteostat', 'weather_data') }}
)

SELECT
    zipcode,
    {{ dbt_utils.generate_surrogate_key(['date']) }} AS date_key,
    avg_temperature_celsius,
    min_temperature_celsius,
    max_temperature_celsius,
    precipitation
FROM src_weather_data