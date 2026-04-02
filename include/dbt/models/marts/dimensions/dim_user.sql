{{ config(
    materialized='table',
    tags=['marts']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_sk,
    user_id,
    email,
    first_name,
    last_name,
    created_at,
    updated_at
FROM {{ ref('stg_user') }}
