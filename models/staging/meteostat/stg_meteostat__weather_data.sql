{{ config(
    materialized='incremental',
    unique_key = 'row_id'
    ) 
}}

WITH src_weather_data AS (
    SELECT *
    FROM {{ source('meteostat', 'weather_data') }}
{% if is_incremental() %}

	  where date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT
    row_id,
    zipcode,
    {{ dbt_utils.generate_surrogate_key(['date']) }} AS date_key,
    avg_temperature_celsius,
    min_temperature_celsius,
    max_temperature_celsius,
    precipitation,
    date_loaded
FROM src_weather_data