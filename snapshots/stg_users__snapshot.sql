{% snapshot stg_users__snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_key',
      strategy='timestamp',
      updated_at='date_loaded',
    )
}}

WITH src_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email,
        phone_number,
        address_id,
        to_date(created_at) AS created_date,
        to_time(created_at) AS created_time_utc,
        to_date(updated_at) AS updated_date,
        to_time(updated_at) AS updated_time_utc,
        _fivetran_synced AS date_loaded
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
    SELECT
         {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
         first_name,
         last_name,
         email,
         phone_number,
         {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
         created_date,
         created_time_utc,
         updated_date,
         updated_time_utc,
         date_loaded
    FROM src_users
    )

SELECT * FROM stg_users

{% endsnapshot %}