{{ config(
    materialized='table',
    tags=['metrics', 'strain', 'recovery']
) }}

WITH readiness_signals AS (
  SELECT 
    r.user_sk,
    r.date_sk,
    
    -- Recovery signals
    r.recovery_score,
    r.recovery_score_percentile,
    r.hrv_rmssd,
    r.skin_temp_deviation,
    
    -- Strain signals
    s.day_strain,
    s.optimal_strain_min,
    s.optimal_strain_max,
    (s.optimal_strain_min + s.optimal_strain_max) / 2 as optimal_strain_target,
    
    -- Recent strain load
    AVG(s.day_strain) OVER (
      PARTITION BY s.user_sk ORDER BY s.date_sk ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
    ) as recent_strain_avg_7d
    
  FROM {{ ref('fact_daily_recovery') }} r
  LEFT JOIN {{ ref('fact_daily_strain') }} s 
    ON r.user_sk = s.user_sk AND r.date_sk = s.date_sk
),

readiness_calculation AS (
  SELECT *,
    -- Body readiness score (0-100, based on recovery)
    recovery_score as body_readiness,
    
    -- Training recommendation score (0-100, based on optimal strain)
    CASE 
      WHEN optimal_strain_target > 15 THEN 90
      WHEN optimal_strain_target > 12 THEN 70
      WHEN optimal_strain_target > 9 THEN 50
      WHEN optimal_strain_target > 6 THEN 30
      ELSE 10
    END as training_recommendation_score,
    
    -- Gap between body and recommendation
    recovery_score - CASE 
      WHEN optimal_strain_target > 15 THEN 90
      WHEN optimal_strain_target > 12 THEN 70
      WHEN optimal_strain_target > 9 THEN 50
      WHEN optimal_strain_target > 6 THEN 30
      ELSE 10
    END as readiness_motivation_gap
    
  FROM readiness_signals
)

SELECT 
  user_sk,
  date_sk,
  recovery_score,
  body_readiness,
  optimal_strain_target,
  training_recommendation_score,
  readiness_motivation_gap,
  
  -- Categorize the gap
  CASE 
    WHEN readiness_motivation_gap > 20 THEN 'Body Ready, Training Light'
    WHEN readiness_motivation_gap > 10 THEN 'Slight Over-recovery'
    WHEN readiness_motivation_gap BETWEEN -10 AND 10 THEN 'Well Aligned'
    WHEN readiness_motivation_gap < -20 THEN 'Body Tired, Training High'
    ELSE 'Slight Under-recovery'
  END as gap_category,
  
  -- Risk flag
  CASE 
    WHEN readiness_motivation_gap < -30 AND recent_strain_avg_7d > 14 THEN TRUE
    ELSE FALSE
  END as overtraining_risk_flag,
  
  CURRENT_TIMESTAMP as calculated_at
  
FROM readiness_calculation