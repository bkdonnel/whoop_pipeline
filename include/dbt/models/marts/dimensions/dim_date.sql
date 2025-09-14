{{ config(materialized='table') }}

WITH base_date_dimension AS (
  {{ dbt_date.get_date_dimension("2020-01-01", "2027-12-31") }}
),

enhanced_for_whoop AS (
  SELECT
    -- Use dbt_utils for surrogate key
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_sk,

    -- All the standard date dimension columns from dbt_date
    *,

    -- Add Whoop-specific enhancements
    CASE
      WHEN month_of_year IN (12, 1, 2) THEN 'Winter'
      WHEN month_of_year IN (3, 4, 5) THEN 'Spring'
      WHEN month_of_year IN (6, 7, 8) THEN 'Summer'
      ELSE 'Fall'
    END as season,

    -- Training periodization
    CASE
      WHEN month_of_year IN (1, 2) THEN 'Base Building'
      WHEN month_of_year IN (3, 4, 5) THEN 'Build Phase'
      WHEN month_of_year IN (6, 7, 8) THEN 'Peak Season'
      ELSE 'Recovery Phase'
    END as training_period,

    -- Days from today for trend analysis
    {{ dbt_date.n_days_away(0, 'date_day') }} as days_from_today

  FROM base_date_dimension
)

SELECT * FROM enhanced_for_whoop