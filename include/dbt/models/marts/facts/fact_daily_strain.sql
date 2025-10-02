{{ config(
    materialized='incremental',
    unique_key='strain_sk',
    on_schema_change='append_new_columns',
    indexes=[
        {'columns': ['user_sk', 'date_sk'], 'unique': True},
        {'columns': ['date_sk']},
        {'columns': ['user_sk']},
        {'columns': ['day_strain']}
    ]
) }}

WITH cycle_data AS (
    SELECT *,
           DATE(CREATED_AT) as cycle_date
    FROM {{ ref('stg_cycle') }}
    {% if is_incremental() %}
        WHERE UPDATED_AT > (
            SELECT COALESCE(MAX(source_updated_at), '1990-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
),

workout_aggregates AS (
    SELECT
        USER_ID,
        DATE(CREATED_AT) as workout_date,
        SUM(STRAIN) as total_workout_strain,
        COUNT(*) as workout_count,
        SUM(DATEDIFF('minute', START_TIME, END_TIME)) as total_workout_minutes,
        SUM(ZONE_ZERO_MILLI) AS zone_0_minutes,
        SUM(ZONE_ONE_MILLI) AS zone_1_minutes,
        SUM(ZONE_TWO_MILLI) AS zone_2_minutes,
        SUM(ZONE_THREE_MILLI) AS zone_3_minutes,
        SUM(ZONE_FOUR_MILLI) AS zone_4_minutes,
        SUM(ZONE_FIVE_MILLI) AS zone_5_minutes,
        MAX(STRAIN) as max_workout_strain,
        ARRAY_AGG(DISTINCT SPORT_NAME) as workout_types
    FROM {{ ref('stg_workout') }}
    WHERE CREATED_AT IS NOT NULL
    GROUP BY USER_ID, DATE(CREATED_AT)
),

cycle_with_keys AS (
    SELECT
        c.*,
        u.user_sk,
        d.date_sk
    FROM cycle_data c
    LEFT JOIN whoop.marts.dim_user u ON u.user_id = c.USER_ID
    LEFT JOIN whoop.marts.dim_date d ON d.date_day = c.cycle_date
    WHERE u.user_sk IS NOT NULL
        AND d.date_sk IS NOT NULL
),

final_data AS (
    SELECT
        c.*,
        w.total_workout_strain,
        w.workout_count,
        w.total_workout_minutes,
        w.max_workout_strain,
        w.workout_types,
        w.zone_0_minutes,
        w.zone_1_minutes,
        w.zone_2_minutes,
        w.zone_3_minutes,
        w.zone_4_minutes,
        w.zone_5_minutes,
        ROW_NUMBER() OVER (PARTITION BY c.user_sk, c.date_sk ORDER BY c.UPDATED_AT DESC) as row_num
    FROM cycle_with_keys c
    LEFT JOIN workout_aggregates w ON w.USER_ID = c.USER_ID AND w.workout_date = c.cycle_date
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['user_sk', 'date_sk']) }} as strain_sk,

    -- Foreign keys
    user_sk,
    date_sk,

    -- Core strain metrics (from cycle)
    STRAIN as day_strain,
    COALESCE(total_workout_strain, 0) as workout_strain,  -- From workout aggregation
    NULL as optimal_strain_min,  -- Not available in current data
    NULL as optimal_strain_max,  -- Not available in current data

    -- Energy expenditure (from cycle)
    KILOJOULE as calories_burned,

    -- Heart rate metrics (from cycle - daily aggregates)
    AVERAGE_HEART_RATE as average_heart_rate,
    MAX_HEART_RATE as max_heart_rate,

    -- Heart rate zones (from cycle - daily totals)
    zone_0_minutes,
    zone_1_minutes,
    zone_2_minutes,
    zone_3_minutes,
    zone_4_minutes,
    zone_5_minutes,

    -- Workout insights (from workout aggregation)
    COALESCE(workout_count, 0) as workout_count,
    total_workout_minutes,
    max_workout_strain,
    workout_types,

    -- Audit fields
    CURRENT_TIMESTAMP as created_timestamp,
    CREATED_AT as source_created_at,
    UPDATED_AT as source_updated_at,
    cycle_date

FROM final_data
WHERE row_num = 1