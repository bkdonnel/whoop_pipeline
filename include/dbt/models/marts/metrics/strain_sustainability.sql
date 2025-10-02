{{ config(
    materialized='table',
    tags=['metrics', 'strain']
) }}

WITH strain_compliance AS (
  SELECT 
    user_sk,
    date_sk,
    day_strain,
    optimal_strain_min,
    optimal_strain_max,
    
    -- Categorize each day
    CASE 
      WHEN day_strain < optimal_strain_min THEN 'Under'
      WHEN day_strain BETWEEN optimal_strain_min AND optimal_strain_max THEN 'Optimal'
      WHEN day_strain > optimal_strain_max THEN 'Over'
    END as strain_category,
    
    -- Calculate deviation
    CASE 
      WHEN day_strain < optimal_strain_min THEN optimal_strain_min - day_strain
      WHEN day_strain > optimal_strain_max THEN day_strain - optimal_strain_max
      ELSE 0
    END as strain_deviation
    
  FROM {{ ref('fact_daily_strain') }}
  WHERE optimal_strain_max IS NOT NULL
),

rolling_sustainability AS (
  SELECT *,
    -- Rolling 30-day metrics
    COUNT(*) OVER (
      PARTITION BY user_sk ORDER BY date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as days_in_window,
    
    COUNT(CASE WHEN strain_category = 'Optimal' THEN 1 END) OVER (
      PARTITION BY user_sk ORDER BY date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as optimal_days_30d,
    
    COUNT(CASE WHEN strain_category = 'Over' THEN 1 END) OVER (
      PARTITION BY user_sk ORDER BY date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as over_days_30d,
    
    AVG(strain_deviation) OVER (
      PARTITION BY user_sk ORDER BY date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as avg_deviation_30d
    
  FROM strain_compliance
)

SELECT 
  user_sk,
  date_sk,
  day_strain,
  optimal_strain_min,
  optimal_strain_max,
  strain_category,
  strain_deviation,
  
  -- Sustainability score (0-100)
  -- High score = consistently in optimal range
  ROUND(
    100 * (optimal_days_30d::FLOAT / NULLIF(days_in_window, 0)) - 
    (over_days_30d::FLOAT / NULLIF(days_in_window, 0) * 50)
  , 2) as sustainability_score_30d,
  
  -- Categorize sustainability
  CASE 
    WHEN (optimal_days_30d::FLOAT / NULLIF(days_in_window, 0)) > 0.7 THEN 'Highly Sustainable'
    WHEN (optimal_days_30d::FLOAT / NULLIF(days_in_window, 0)) > 0.5 THEN 'Sustainable'
    WHEN (optimal_days_30d::FLOAT / NULLIF(days_in_window, 0)) > 0.3 THEN 'Moderately Sustainable'
    ELSE 'Unsustainable'
  END as sustainability_category,
  
  CURRENT_TIMESTAMP as calculated_at
  
FROM rolling_sustainability