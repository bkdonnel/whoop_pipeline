{{ config(
    materialized='table',
    tags=['metrics', 'recovery', 'seasonal']
) }}

WITH seasonal_baselines AS (
  SELECT 
    r.user_sk,
    d.season,
    d.year_number,
    
    -- Seasonal averages
    AVG(r.recovery_score) as avg_recovery,
    AVG(r.hrv_rmssd) as avg_hrv,
    AVG(r.resting_heart_rate) as avg_rhr,
    AVG(r.skin_temp_celsius) as avg_skin_temp,
    
    -- Seasonal variability
    STDDEV(r.recovery_score) as recovery_stddev,
    COUNT(*) as sample_size
    
  FROM {{ ref('fact_daily_recovery') }} r
  JOIN whoop.marts.dim_date d ON r.date_sk = d.date_sk
  GROUP BY r.user_sk, d.season, d.year_number
),

user_seasonal_patterns AS (
  SELECT 
    user_sk,
    season,
    
    -- Average across all years for this season
    AVG(avg_recovery) as seasonal_recovery_baseline,
    AVG(avg_hrv) as seasonal_hrv_baseline,
    AVG(avg_rhr) as seasonal_rhr_baseline,
    AVG(avg_skin_temp) as seasonal_skin_temp_baseline
    
  FROM seasonal_baselines
  GROUP BY user_sk, season
),

current_vs_seasonal AS (
  SELECT 
    r.user_sk,
    r.date_sk,
    d.season,
    r.recovery_score,
    r.hrv_rmssd,
    r.resting_heart_rate,
    r.skin_temp_celsius,
    
    sp.seasonal_recovery_baseline,
    sp.seasonal_hrv_baseline,
    sp.seasonal_rhr_baseline,
    sp.seasonal_skin_temp_baseline,
    
    -- Deviation from seasonal baseline
    r.recovery_score - sp.seasonal_recovery_baseline as recovery_deviation,
    r.hrv_rmssd - sp.seasonal_hrv_baseline as hrv_deviation,
    sp.seasonal_rhr_baseline - r.resting_heart_rate as rhr_deviation,  -- Lower RHR is better
    r.skin_temp_celsius - sp.seasonal_skin_temp_baseline as skin_temp_deviation
    
  FROM {{ ref('fact_daily_recovery') }} r
  JOIN whoop.marts.dim_date d ON r.date_sk = d.date_sk
  LEFT JOIN user_seasonal_patterns sp 
    ON r.user_sk = sp.user_sk AND d.season = sp.season
)

SELECT 
  user_sk,
  date_sk,
  season,
  recovery_score,
  seasonal_recovery_baseline,
  recovery_deviation,
  
  -- Composite seasonal adaptation index
  ROUND(
    (recovery_deviation * 0.4 + 
     hrv_deviation * 0.3 + 
     rhr_deviation * 0.2 + 
     skin_temp_deviation * 0.1) / 4
  , 2) as seasonal_adaptation_index,
  
  -- Categorize adaptation
  CASE 
    WHEN recovery_deviation > 10 THEN 'Thriving'
    WHEN recovery_deviation > 5 THEN 'Well Adapted'
    WHEN recovery_deviation > -5 THEN 'Normal Adaptation'
    WHEN recovery_deviation > -10 THEN 'Struggling'
    ELSE 'Poorly Adapted'
  END as adaptation_category,
  
  CURRENT_TIMESTAMP as calculated_at
  
FROM current_vs_seasonal