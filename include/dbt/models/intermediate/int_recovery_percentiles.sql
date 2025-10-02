{{
    config(
        materialized='incremental',
        unique_key='date_sk',
        tags=['staging', 'intermediate'],
        incremental_strategy='merge'
    )
}}

WITH recovery_data AS (
    SELECT
        u.user_sk
       ,d.date_sk
       ,DATE(r.CREATED_AT) as recovery_date
       ,r.RECOVERY_SCORE as recovery_score
       ,r.HRV_RMSSD_MILLI as hrv_rmssd
       ,r.RESTING_HEART_RATE as resting_heart_rate
       ,r.SKIN_TEMP_CELSIUS as skin_temp_celsius
       ,r.CREATED_AT
    FROM {{ ref('stg_recovery') }} r
    LEFT JOIN whoop.marts.dim_user u ON r.USER_ID = u.user_id
    LEFT JOIN whoop.marts.dim_date d ON DATE(r.CREATED_AT) = d.date_day
    WHERE r.RECOVERY_SCORE IS NOT NULL
        AND u.user_sk IS NOT NULL
        AND d.date_sk IS NOT NULL

    {% if is_incremental() %}
    -- Only process last 90 days to keep percentiles fresh with reasonable memory
    AND DATE(r.CREATED_AT) >= CURRENT_DATE - 90
    {% endif %}
),

-- Calculate skin temp baseline and deviation
skin_temp_with_baseline AS (
    SELECT 
        *,
        -- Calculate 30-day rolling baseline skin temp (excluding current day)
        AVG(skin_temp_celsius) OVER (
            PARTITION BY user_sk 
            ORDER BY date_sk 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) as baseline_skin_temp,
        
        -- Count days of history for baseline calculation
        COUNT(skin_temp_celsius) OVER (
            PARTITION BY user_sk 
            ORDER BY date_sk 
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) as baseline_days_count
        
    FROM recovery_data
    WHERE skin_temp_celsius IS NOT NULL
),

recovery_with_skin_deviation AS (
    SELECT 
        *,
        -- Calculate deviation from personal baseline
        CASE 
            WHEN baseline_skin_temp IS NOT NULL AND baseline_days_count >= 14 THEN 
                ROUND(skin_temp_celsius - baseline_skin_temp, 2)
            ELSE NULL 
        END as skin_temp_deviation
        
    FROM skin_temp_with_baseline
),

percentile_calculations AS (
    SELECT 
        user_sk
       ,date_sk
       ,recovery_score
       ,hrv_rmssd
       ,resting_heart_rate
       ,skin_temp_celsius
       ,skin_temp_deviation  -- Include the calculated deviation
       -- Proper percentile calculation using only preceding rows (excludes current row)
       ,ROUND(PERCENT_RANK() OVER (PARTITION BY user_sk ORDER BY recovery_score) * 100)::INTEGER AS recovery_score_percentile
       ,ROUND(PERCENT_RANK() OVER (PARTITION BY user_sk ORDER BY hrv_rmssd) * 100)::INTEGER AS hrv_percentile
       ,ROUND(PERCENT_RANK() OVER (PARTITION BY user_sk ORDER BY resting_heart_rate DESC) * 100)::INTEGER AS rhr_percentile
       ,COUNT(*) OVER (PARTITION BY user_sk ORDER BY date_sk ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as days_of_history
    FROM recovery_with_skin_deviation
)

SELECT 
    user_sk
   ,date_sk
   -- Only show percentiles after 60 days of data
   ,CASE WHEN days_of_history >= 60 THEN recovery_score_percentile ELSE NULL END as recovery_score_percentile
   ,CASE WHEN days_of_history >= 60 THEN hrv_percentile ELSE NULL END as hrv_percentile
   ,CASE WHEN days_of_history >= 60 THEN rhr_percentile ELSE NULL END as rhr_percentile
   -- Include skin temperature data
   ,skin_temp_celsius
   ,skin_temp_deviation
   ,days_of_history
FROM percentile_calculations