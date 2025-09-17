-- One-time creation of dim_date table for Whoop pipeline
-- Run this directly in Snowflake to create the dimension table

CREATE OR REPLACE TABLE whoop.marts.dim_date AS
WITH date_spine AS (
  SELECT
    dateadd(day, seq4(), '2020-01-01'::date) as date_day
  FROM table(generator(rowcount => 2922))  -- 2020-01-01 to 2027-12-31 = 2922 days
),

date_dimension AS (
  SELECT
    date_day,

    -- Basic date parts
    year(date_day) as year_number,
    quarter(date_day) as quarter_of_year,
    month(date_day) as month_of_year,
    monthname(date_day) as month_name,
    day(date_day) as day_of_month,
    dayofweek(date_day) as day_of_week,
    dayname(date_day) as day_name,
    dayofyear(date_day) as day_of_year,

    -- Week calculations
    weekofyear(date_day) as week_of_year,

    -- Business day logic
    CASE WHEN dayofweek(date_day) IN (1, 7) THEN false ELSE true END as is_weekday,

    -- Date formatting
    to_char(date_day, 'YYYY-MM-DD') as date_day_name,
    to_char(date_day, 'MON YYYY') as month_year_name,

    -- Season calculation
    CASE
      WHEN month(date_day) IN (12, 1, 2) THEN 'Winter'
      WHEN month(date_day) IN (3, 4, 5) THEN 'Spring'
      WHEN month(date_day) IN (6, 7, 8) THEN 'Summer'
      ELSE 'Fall'
    END as season,

    -- Training periodization for Whoop data
    CASE
      WHEN month(date_day) IN (1, 2) THEN 'Base Building'
      WHEN month(date_day) IN (3, 4, 5) THEN 'Build Phase'
      WHEN month(date_day) IN (6, 7, 8) THEN 'Peak Season'
      ELSE 'Recovery Phase'
    END as training_period,

    -- Days from today for trend analysis
    datediff(day, current_date(), date_day) as days_from_today

  FROM date_spine
)

SELECT
  -- Surrogate key (simple hash of date)
  hash(date_day) as date_sk,
  *
FROM date_dimension
ORDER BY date_day;

-- Verify the table was created
SELECT
  count(*) as total_rows,
  min(date_day) as min_date,
  max(date_day) as max_date
FROM whoop.marts.dim_date;