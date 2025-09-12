{{ config(
    materialized='incremental',
    unique_key='ID',
    on_schema_change='fail'
) }}

{# Static column definitions for sleep table #}
{% set column_casts = {
    'ID': 'string',
    'V1_ID': 'string', 
    'USER_ID': 'string',
    'CREATED_AT': 'timestamp_ntz',
    'UPDATED_AT': 'timestamp_ntz',
    'START_TIME': 'timestamp_ntz',
    'END_TIME': 'timestamp_ntz',
    'TIMEZONE_OFFSET': 'string',
    'NAP': 'boolean',
    'SCORE_STATE': 'string',
    'TOTAL_IN_BED_TIME_MILLI': 'int',
    'TOTAL_AWAKE_TIME_MILLI': 'int', 
    'TOTAL_NO_DATA_TIME_MILLI': 'int',
    'TOTAL_LIGHT_SLEEP_TIME_MILLI': 'int',
    'TOTAL_SLOW_WAVE_SLEEP_TIME_MILLI': 'int',
    'TOTAL_REM_SLEEP_TIME_MILLI': 'int',
    'SLEEP_CYCLE_COUNT': 'int',
    'DISTURBANCE_COUNT': 'int',
    'BASELINE_MILLI': 'int',
    'NEED_FROM_SLEEP_DEBT_MILLI': 'int',
    'NEED_FROM_RECENT_STRAIN_MILLI': 'int', 
    'NEED_FROM_RECENT_NAP_MILLI': 'int',
    'RESPIRATORY_RATE': 'int',
    'SLEEP_PERFORMANCE_PERCENTAGE': 'int',
    'SLEEP_CONSISTENCY_PERCENTAGE': 'int',
    'SLEEP_EFFICIENCY_PERCENTAGE': 'int',
    'INGESTED_AT': 'timestamp_ntz'
} %}

{# Custom transformations for ms to hours/minutes #}
{% set transformations = {
    'TOTAL_IN_BED_TIME_MILLI': 'TOTAL_IN_BED_TIME_MILLI / 3600000',
    'TOTAL_AWAKE_TIME_MILLI': 'TOTAL_AWAKE_TIME_MILLI / 60000',
    'TOTAL_NO_DATA_TIME_MILLI': 'TOTAL_NO_DATA_TIME_MILLI / 60000',
    'TOTAL_LIGHT_SLEEP_TIME_MILLI': 'TOTAL_LIGHT_SLEEP_TIME_MILLI / 3600000',
    'TOTAL_SLOW_WAVE_SLEEP_TIME_MILLI': 'TOTAL_SLOW_WAVE_SLEEP_TIME_MILLI / 3600000',
    'TOTAL_REM_SLEEP_TIME_MILLI': 'TOTAL_REM_SLEEP_TIME_MILLI / 3600000',
    'BASELINE_MILLI':'BASELINE_MILLI / 3600000',
    'NEED_FROM_SLEEP_DEBT_MILLI':'NEED_FROM_SLEEP_DEBT_MILLI / 3600000',
    'NEED_FROM_RECENT_STRAIN_MILLI':'NEED_FROM_RECENT_STRAIN_MILLI / 3600000',
    'NEED_FROM_RECENT_NAP_MILLI':'NEED_FROM_RECENT_NAP_MILLI / 3600000'
} %}

WITH ranked_data AS (
    {{ generate_staging_model('raw', 'sleep', column_casts, transformations) }}
    
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