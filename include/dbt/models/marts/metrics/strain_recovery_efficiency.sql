{{ config(
    materialized='table',
    tags=['metrics', 'strain', 'recovery']
) }}

WITH strain_recovery_pairs AS (
  SELECT 
    s.user_sk,
    s.date_sk,
    s.day_strain,
    r.recovery_score,
    
    -- Get next day's recovery
    LEAD(r.recovery_score) OVER (
      PARTITION BY s.user_sk ORDER BY s.date_sk
    ) as next_day_recovery,
    
    -- Get baseline recovery (7-day trailing average)
    AVG(r.recovery_score) OVER (
      PARTITION BY r.user_sk 
      ORDER BY r.date_sk 
      ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) as baseline_recovery
    
  FROM {{ ref('fact_daily_strain') }} s
  LEFT JOIN {{ ref('fact_daily_recovery') }} r 
    ON s.user_sk = r.user_sk AND s.date_sk = r.date_sk
),

efficiency_calculation AS (
  SELECT *,
    -- Recovery change per unit of strain
    CASE 
      WHEN day_strain > 0 AND next_day_recovery IS NOT NULL AND baseline_recovery IS NOT NULL
      THEN (next_day_recovery - baseline_recovery) / day_strain
      ELSE NULL 
    END as recovery_efficiency_ratio
  FROM strain_recovery_pairs
)

SELECT 
  user_sk,
  date_sk,
  day_strain,
  recovery_score,
  next_day_recovery,
  baseline_recovery,
  recovery_efficiency_ratio,
  
  -- Rolling 30-day efficiency average
  AVG(recovery_efficiency_ratio) OVER (
    PARTITION BY user_sk 
    ORDER BY date_sk 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) as efficiency_30day_avg,
  
  -- Categorize efficiency
  CASE
    WHEN recovery_efficiency_ratio > 1 THEN 'Highly Efficient'
    WHEN recovery_efficiency_ratio > 0.5 THEN 'Efficient'
    WHEN recovery_efficiency_ratio > 0 THEN 'Moderate'
    WHEN recovery_efficiency_ratio > -0.5 THEN 'Poor'
    ELSE 'Very Poor'
  END as efficiency_category,
  
  CURRENT_TIMESTAMP as calculated_at
  
FROM efficiency_calculation