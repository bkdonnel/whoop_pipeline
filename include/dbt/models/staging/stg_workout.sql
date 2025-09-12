{{ config(
    materialized='incremental',
    unique_key='ID',
    on_schema_change='fail'
) }}

{# Static column definitions for workout table #}
{% set column_casts = {
    'ID': 'string',
    'V1_ID': 'string',
    'USER_ID': 'string',
    'CREATED_AT': 'timestamp_ntz',
    'UPDATED_AT': 'timestamp_ntz',
    'START_TIME': 'timestamp_ntz',
    'END_TIME': 'timestamp_ntz',
    'TIMEZONE_OFFSET': 'string',
    'SPORT_NAME': 'string',
    'SPORT_ID': 'string',
    'SCORE_STATE': 'string',
    'STRAIN': 'int',
    'AVERAGE_HEART_RATE': 'int',
    'MAX_HEART_RATE': 'int',
    'KILOJOULE': 'int',
    'PERCENT_RECORDED': 'int',
    'DISTANCE_METER': 'int',
    'ALTITUDE_GAIN_METER': 'int',
    'ALTITUDE_CHANGE_METER': 'int',
    'ZONE_ZERO_MILLI': 'int',
    'ZONE_ONE_MILLI': 'int',
    'ZONE_TWO_MILLI': 'int',
    'ZONE_THREE_MILLI': 'int',
    'ZONE_FOUR_MILLI': 'int',
    'ZONE_FIVE_MILLI': 'int',
    'INGESTED_AT': 'timestamp_ntz'
} %}

{# Custom transformations for ms to hours/minutes #}
{% set transformations = {
    'ZONE_ZERO_MILLI': 'ZONE_ZERO_MILLI / 60000',
    'ZONE_ONE_MILLI': 'ZONE_ONE_MILLI / 60000',
    'ZONE_TWO_MILLI': 'ZONE_TWO_MILLI / 60000',
    'ZONE_THREE_MILLI': 'ZONE_THREE_MILLI / 60000',
    'ZONE_FOUR_MILLI': 'ZONE_FOUR_MILLI / 60000',
    'ZONE_FIVE_MILLI': 'ZONE_FIVE_MILLI / 60000'
} %}

WITH ranked_data AS (
    {{ generate_staging_model('raw', 'workout', column_casts, transformations) }}
    
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