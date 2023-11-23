{{ config(
  materialized='table'
) }}

WITH generate_date AS 
(
  {{ dbt_date.get_date_dimension("2020-01-01", "2050-12-31") }}
),

dim_date_with_null AS (
    SELECT
        date_day,
        date_day as date,
        day_of_month,
        month_of_year,
        year_number,
        day_of_week_name as day_of_week,
        week_of_year,
        quarter_of_year
    FROM generate_date
    UNION ALL
    SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
    date,
    day_of_month,
    month_of_year,
    year_number,
    day_of_week,
    week_of_year,
    quarter_of_year
FROM dim_date_with_null