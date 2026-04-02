{{ config(
    materialized='table',
    tags=['marts']
) }}

WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2020-01-01'::date) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 4020))
)

SELECT
    REPLACE(CAST(date_day AS VARCHAR), '-', '')::INTEGER AS date_sk,
    date_day,
    DAYOFWEEK(date_day) AS day_of_week,
    DAYNAME(date_day) AS day_of_week_name,
    DAY(date_day) AS day_of_month,
    DAYOFYEAR(date_day) AS day_of_year,
    WEEKOFYEAR(date_day) AS week_of_year,
    MONTH(date_day) AS month_of_year,
    MONTHNAME(date_day) AS month_name,
    QUARTER(date_day) AS quarter_of_year,
    YEAR(date_day) AS year_number,
    CASE
        WHEN MONTH(date_day) IN (12, 1, 2)  THEN 'Winter'
        WHEN MONTH(date_day) IN (3, 4, 5)   THEN 'Spring'
        WHEN MONTH(date_day) IN (6, 7, 8)   THEN 'Summer'
        WHEN MONTH(date_day) IN (9, 10, 11) THEN 'Fall'
    END AS season
FROM date_spine
WHERE date_day <= '2030-12-31'
