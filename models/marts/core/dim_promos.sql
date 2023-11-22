{{
  config(
    materialized='table'
  )
}}

WITH stg_promos AS 
(
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__promos') }}
)

SELECT *
FROM stg_promos