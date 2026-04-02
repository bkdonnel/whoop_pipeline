{{ config(
    materialized='table',
    tags=['marts']
) }}

WITH date_spine AS (
    {{ dbt_date.get_date_dimension('2020-01-01', '2030-12-31') }}
)

SELECT
    REPLACE(CAST(date_day AS VARCHAR), '-', '')::INTEGER AS date_sk,
    date_day,
    day_of_week,
    day_of_week_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_of_year,
    month_name,
    quarter_of_year,
    year_number,
    CASE
        WHEN month_of_year IN (12, 1, 2)  THEN 'Winter'
        WHEN month_of_year IN (3, 4, 5)   THEN 'Spring'
        WHEN month_of_year IN (6, 7, 8)   THEN 'Summer'
        WHEN month_of_year IN (9, 10, 11) THEN 'Fall'
    END AS season
FROM date_spine
