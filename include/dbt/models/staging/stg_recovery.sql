{{ config(
    materialized='incremental',
    unique_key='CYCLE_ID',
    on_schema_change='fail'
) }}

{# Static column definitions for recovery table #}
{% set column_casts = {
    'CYCLE_ID': 'string',
    'SLEEP_ID': 'string',
    'USER_ID': 'string',
    'CREATED_AT': 'timestamp_ntz',
    'UPDATED_AT': 'timestamp_ntz',
    'SCORE_STATE': 'string',
    'USER_CALIBRATING': 'boolean',
    'RECOVERY_SCORE': 'int',
    'RESTING_HEART_RATE': 'int',
    'HRV_RMSSD_MILLI': 'int',
    'SPO2_PERCENTAGE': 'int',
    'SKIN_TEMP_CELSIUS': 'int',
    'INGESTED_AT': 'timestamp_ntz'
} %}

{# No transformations needed for recovery #}
{% set transformations = {} %}

WITH ranked_data AS (
    {{ generate_staging_model('raw', 'raw_recovery', column_casts, transformations) }}

    {% if is_incremental() %}
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CYCLE_ID ORDER BY ingested_at DESC, UPDATED_AT DESC) AS rn
    FROM ranked_data
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1