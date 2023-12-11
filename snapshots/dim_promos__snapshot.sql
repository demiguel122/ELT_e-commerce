{% snapshot dim_promos__snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='promo_key',
      strategy='timestamp',
      updated_at='date_loaded',
    )
}}

WITH stg_promos AS 
(
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__promos') }}
)

SELECT *
FROM stg_promos

{% endsnapshot %}