{{
  config(
    materialized='table'
  )
}}

WITH int_most_sold_product_per_zipcode AS (
    SELECT *
    FROM {{ ref('int_most_sold_product_per_zipcode') }}
),

stg_weather_data AS (
    SELECT *
    FROM {{ ref('stg_meteostat__weather_data') }}
)

SELECT
    a.zipcode,
    a.created_date_key AS date_key,
    a.most_sold_product_key,
    a.units_sold,
    b.avg_temperature_celsius,
    b.min_temperature_celsius,
    b.max_temperature_celsius,
    b.precipitation
FROM int_most_sold_product_per_zipcode AS a
JOIN stg_weather_data AS b
ON a.zipcode = b.zipcode AND a.created_date_key = b.date_key