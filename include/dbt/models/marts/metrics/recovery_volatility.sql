{{ config(
    materialized='table',
    tags=['metrics', 'recovery']
) }}

WITH volatility_calculations AS (
  SELECT 
    user_sk,
    date_sk,
    recovery_score,
    
    -- Short-term volatility (7 days)
    STDDEV(recovery_score) OVER (
      PARTITION BY user_sk 
      ORDER BY date_sk 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as volatility_7d,
    
    -- Medium-term volatility (30 days)
    STDDEV(recovery_score) OVER (
      PARTITION BY user_sk 
      ORDER BY date_sk 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as volatility_30d,
    
    -- Average recovery for coefficient of variation
    AVG(recovery_score) OVER (
      PARTITION BY user_sk 
      ORDER BY date_sk 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as avg_recovery_30d
    
  FROM {{ ref('fact_daily_recovery') }}
)

SELECT 
  user_sk,
  date_sk,
  recovery_score,
  volatility_7d,
  volatility_30d,
  
  -- Coefficient of variation (normalized volatility)
  volatility_30d / NULLIF(avg_recovery_30d, 0) as volatility_coefficient,
  
  -- Volatility trend (is it increasing or decreasing?)
  volatility_7d - LAG(volatility_7d, 7) OVER (
    PARTITION BY user_sk ORDER BY date_sk
  ) as volatility_trend,
  
  -- Categorize volatility
  CASE 
    WHEN volatility_30d < 5 THEN 'Very Stable'
    WHEN volatility_30d < 10 THEN 'Stable'
    WHEN volatility_30d < 15 THEN 'Moderate'
    WHEN volatility_30d < 20 THEN 'Volatile'
    ELSE 'Very Volatile'
  END as volatility_category,
  
  CURRENT_TIMESTAMP as calculated_at
  
FROM volatility_calculations