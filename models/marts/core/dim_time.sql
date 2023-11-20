{{ config(
  materialized='table'
) }}

WITH generate_time AS 
(
    {{ dbt_utils.date_spine(
    datepart="second",
    start_date="cast('00:00:00' as time)",
    end_date="cast('23:59:59' as time)"
   )
}}
),

dim_time_with_null AS (
    SELECT
        date_second,
        date_second AS time_utc,
        CASE 
            WHEN date_second < '12:00:00' THEN 'am'
            ELSE 'pm' END AS am_or_pm
    FROM generate_time
    UNION ALL
    SELECT NULL, NULL, NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_second']) }} AS time_key,
    time_utc,
    am_or_pm
FROM dim_time_with_null