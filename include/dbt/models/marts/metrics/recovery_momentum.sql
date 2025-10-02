{{
    config(
        materialized='table',
        tags=['metrics','recovery']
    )
}}


WITH recovery_changes AS (
    SELECT
        user_sk
       ,date_sk
       ,recovery_score

       -- Calculate changes over different time periods
       ,recovery_score - LAG(recovery_score, 1) OVER (PARTITION BY user_sk ORDER BY date_sk) AS day_1_change
       ,recovery_score - LAG(recovery_score, 3) OVER (PARTITION BY user_sk ORDER BY date_sk) AS day_3_change
       ,recovery_score - LAG(recovery_score, 7) OVER (PARTITION BY user_sk ORDER BY date_sk) AS day_7_change
       ,recovery_score - LAG(recovery_score, 14) OVER (PARTITION BY user_sk ORDER BY date_sk) AS day_14_change

    FROM {{ ref('fact_daily_recovery') }}
),


momentum_calculation AS (
  SELECT *,
    -- Weighted momentum: recent changes matter more
    (COALESCE(day_1_change, 0) * 0.4 + 
     COALESCE(day_3_change, 0) * 0.3 + 
     COALESCE(day_7_change, 0) * 0.2 + 
     COALESCE(day_14_change, 0) * 0.1) / 4 as raw_momentum
  FROM recovery_changes
)


SELECT 
   user_sk
  ,date_sk
  ,recovery_score
  
  -- Individual change components
  ,day_1_change
  ,day_3_change
  ,day_7_change
  ,day_14_change
  
  -- Momentum score
  ,raw_momentum
  ,GREATEST(-100, LEAST(100, raw_momentum * 10)) as recovery_momentum_score
  
  -- Categorize momentum
  ,CASE 
     WHEN raw_momentum > 2 THEN 'Accelerating'
     WHEN raw_momentum > 0.5 THEN 'Improving'
     WHEN raw_momentum > -0.5 THEN 'Stable'
     WHEN raw_momentum > -2 THEN 'Declining'
     ELSE 'Deteriorating'
   END as momentum_category
  
  ,CURRENT_TIMESTAMP as calculated_at
  
FROM momentum_calculation