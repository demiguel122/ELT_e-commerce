{{
  config(
    materialized='table'
  )
}}

WITH stg_event_types AS (
    SELECT DISTINCT event_type_key
    FROM {{ ref('stg_sql_server_dbo__events') }}
)

SELECT *
FROM stg_event_types