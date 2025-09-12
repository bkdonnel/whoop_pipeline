{{ config(
    materialized='incremental',
    unique_key='ID',
    on_schema_change='fail',
    merge_update_columns=['UPDATED_AT', 'INGESTED_AT']
) }}

{# Static column definitions for cycle table #}
{% set column_casts = {
    'ID': 'string',
    'USER_ID': 'string',
    'CREATED_AT': 'timestamp_ntz',
    'UPDATED_AT': 'timestamp_ntz',
    'START_TIME': 'timestamp_ntz',
    'END_TIME': 'timestamp_ntz',
    'TIMEZONE_OFFSET': 'string',
    'SCORE_STATE': 'string',
    'STRAIN': 'int',
    'KILOJOULE': 'int',
    'AVERAGE_HEART_RATE': 'int',
    'MAX_HEART_RATE': 'int',
    'INGESTED_AT': 'timestamp_ntz'
} %}

{# No transformations needed for cycle #}
{% set transformations = {} %}

WITH ranked_data AS (
    {{ generate_staging_model('raw', 'cycle', column_casts, transformations) }}
    
    {% if is_incremental() %}
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ingested_at DESC, UPDATED_AT DESC) as rn
    FROM ranked_data
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1