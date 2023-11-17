{{ config(
  materialized='table'
) }}

WITH dim_date AS 
(
  {{ dbt_date.get_date_dimension("2020-01-01", "2050-12-31") }}
)

SELECT
    date_day as date,
    day_of_month,
    month_of_year,
    year_number,
    day_of_week_name as day_of_week,
    week_of_year,
    quarter_of_year
FROM dim_date