{{ config(
    materialized='incremental',
    unique_key='recovery_sk',
    on_schema_change='append_new_columns',
    indexes=[
        {'columns': ['user_sk', 'date_sk'], 'unique': True},
        {'columns': ['date_sk']},
        {'columns': ['user_sk']},
        {'columns': ['recovery_score']},
        {'columns': ['created_at']}
    ]
) }}

WITH recovery_base AS (
    SELECT
        CYCLE_ID,
        USER_ID,
        CREATED_AT,
        UPDATED_AT,
        SCORE_STATE,
        USER_CALIBRATING,
        RECOVERY_SCORE,
        RESTING_HEART_RATE,
        HRV_RMSSD_MILLI,
        SPO2_PERCENTAGE,
        SKIN_TEMP_CELSIUS,
        INGESTED_AT
    FROM {{ ref('stg_recovery') }}

    {% if is_incremental() %}
    WHERE INGESTED_AT > (SELECT COALESCE(MAX(created_timestamp), '1900-01-01') FROM {{ this }})
    {% endif %}
),

-- Join with existing dim_user to get proper user_sk
fact_recovery AS (
    SELECT
        -- Surrogate keys
        HASH(CONCAT(r.CYCLE_ID, '|', r.USER_ID)) AS recovery_sk,
        u.user_sk,
        d.date_sk,

        -- Recovery metrics
        r.RECOVERY_SCORE::DECIMAL(5,2) AS recovery_score,
        rp.recovery_score_percentile,
        (r.HRV_RMSSD_MILLI / 1000.0)::DECIMAL(8,3) AS hrv_rmssd,  -- Convert from milliseconds
        rp.hrv_percentile,
        r.RESTING_HEART_RATE::INTEGER AS resting_heart_rate,
        rp.rhr_percentile,

        -- Sleep performance metrics from sleep data
        s.SLEEP_PERFORMANCE_PERCENTAGE::DECIMAL(5,2) AS sleep_performance_percentage,
        s.SLEEP_CONSISTENCY_PERCENTAGE::DECIMAL(5,2) AS sleep_consistency_percentage,
        s.SLEEP_EFFICIENCY_PERCENTAGE::DECIMAL(5,2) AS sleep_efficiency_percentage,

        -- Skin temperature
        r.SKIN_TEMP_CELSIUS::DECIMAL(4,2) AS skin_temp_celsius,
        rp.skin_temp_deviation::DECIMAL(4,2) AS skin_temp_deviation,  -- Would need baseline calculation

        -- Respiratory rate (not available in current recovery table)
        s.respiratory_rate::DECIMAL(4,2) AS respiratory_rate,

        -- Metadata
        r.CREATED_AT AS created_timestamp,
        r.UPDATED_AT AS source_updated_at,
        r.USER_ID AS source_user_id,
        r.CYCLE_ID AS source_cycle_id

    FROM recovery_base r
    INNER JOIN whoop.marts.dim_user u
        ON r.USER_ID = u.user_id
        AND u.is_current = TRUE  -- Only join with current user records
    INNER JOIN whoop.marts.dim_date d
        ON DATE(r.CREATED_AT) = d.date_day
    LEFT JOIN {{ ref('int_recovery_percentiles') }} rp
        ON u.user_sk = rp.user_sk
        AND d.date_sk = rp.date_sk
    LEFT JOIN {{ ref('stg_sleep') }} s
        ON r.USER_ID = s.USER_ID
        AND DATE(r.CREATED_AT) = DATE(s.CREATED_AT)
    WHERE r.RECOVERY_SCORE IS NOT NULL  -- Only include records with recovery scores
),

-- Deduplicate based on latest update time
final AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY user_sk, date_sk ORDER BY source_updated_at DESC, created_timestamp DESC) AS rn
    FROM fact_recovery
)

SELECT * EXCLUDE (rn)
FROM final
WHERE rn = 1