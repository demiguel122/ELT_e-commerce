{{
  config(
    materialized='table'
  )
}}

WITH distinct_event_type AS (
    SELECT DISTINCT event_type, page_url 
    FROM {{ ref('stg_sql_server_dbo__events') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['event_type', 'page_url']) }} AS event_type_key,
    event_type,
    page_url
FROM distinct_event_type