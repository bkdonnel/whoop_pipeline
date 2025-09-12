{{ config(
    materialized='table',
    tags=['staging', 'user']
) }}

{# User profile - full refresh each time #}
{% set column_casts = {
    'USER_ID': 'string',
    'EMAIL': 'string', 
    'FIRST_NAME': 'string',
    'LAST_NAME': 'string',
    'CREATED_AT': 'timestamp_ntz',
    'UPDATED_AT': 'timestamp_ntz',
    'INGESTED_AT': 'timestamp_ntz'
} %}

{# No transformations needed for user #}
{% set transformations = {} %}

{{ generate_staging_model('raw', 'user', column_casts, transformations) }}