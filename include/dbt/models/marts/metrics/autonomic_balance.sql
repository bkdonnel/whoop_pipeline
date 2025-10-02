{{ config(
    materialized='table',
    tags=['metrics', 'recovery', 'hrv']
) }}

WITH autonomic_metrics AS (
  SELECT 
    user_sk,
    date_sk,
    recovery_score,
    hrv_rmssd,
    resting_heart_rate,
    respiratory_rate,
    
    -- Normalize each metric to 0-100 scale using percentiles
    hrv_percentile,
    rhr_percentile,
    
    -- Calculate respiratory rate percentile (if available)
    PERCENT_RANK() OVER (
      PARTITION BY user_sk ORDER BY respiratory_rate DESC
    ) * 100 as respiratory_percentile
    
  FROM {{ ref('fact_daily_recovery') }}
  WHERE hrv_rmssd IS NOT NULL 
    AND resting_heart_rate IS NOT NULL
),

balance_calculation AS (
  SELECT *,
    -- Composite autonomic balance score (weighted average of percentiles)
    ROUND(
      (COALESCE(hrv_percentile, 50) * 0.45 +     -- HRV most important
       COALESCE(rhr_percentile, 50) * 0.35 +     -- RHR second
       COALESCE(respiratory_percentile, 50) * 0.20)  -- Respiratory third
    , 2) as autonomic_balance_score,
    
    -- HRV/RHR ratio (another autonomic indicator)
    hrv_rmssd / NULLIF(resting_heart_rate, 0) as hrv_rhr_ratio,
    
    -- Rolling 7-day average for stability
    AVG(hrv_rmssd / NULLIF(resting_heart_rate, 0)) OVER (
      PARTITION BY user_sk ORDER BY date_sk ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as hrv_rhr_ratio_7d_avg
    
  FROM autonomic_metrics
)

SELECT 
  user_sk,
  date_sk,
  recovery_score,
  hrv_rmssd,
  hrv_percentile,
  resting_heart_rate,
  rhr_percentile,
  respiratory_rate,
  respiratory_percentile,
  
  autonomic_balance_score,
  hrv_rhr_ratio,
  hrv_rhr_ratio_7d_avg,
  
  -- Categorize autonomic balance
  CASE 
    WHEN autonomic_balance_score >= 80 THEN 'Excellent Balance'
    WHEN autonomic_balance_score >= 60 THEN 'Good Balance'
    WHEN autonomic_balance_score >= 40 THEN 'Moderate Balance'
    WHEN autonomic_balance_score >= 20 THEN 'Poor Balance'
    ELSE 'Very Poor Balance'
  END as balance_category,
  
  CURRENT_TIMESTAMP as calculated_at
  
FROM balance_calculation