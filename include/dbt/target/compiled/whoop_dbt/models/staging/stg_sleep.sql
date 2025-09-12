







WITH ranked_data AS (
    
    SELECT
    
            ID::string AS ID
        ,
    
            V1_ID::string AS V1_ID
        ,
    
            USER_ID::string AS USER_ID
        ,
    
            CREATED_AT::timestamp_ntz AS CREATED_AT
        ,
    
            UPDATED_AT::timestamp_ntz AS UPDATED_AT
        ,
    
            START_TIME::timestamp_ntz AS START_TIME
        ,
    
            END_TIME::timestamp_ntz AS END_TIME
        ,
    
            TIMEZONE_OFFSET::string AS TIMEZONE_OFFSET
        ,
    
            NAP::boolean AS NAP
        ,
    
            SCORE_STATE::string AS SCORE_STATE
        ,
    
            TOTAL_IN_BED_TIME_MILLI / 3600000 AS TOTAL_IN_BED_TIME_MILLI
        ,
    
            TOTAL_AWAKE_TIME_MILLI / 60000 AS TOTAL_AWAKE_TIME_MILLI
        ,
    
            TOTAL_NO_DATA_TIME_MILLI / 60000 AS TOTAL_NO_DATA_TIME_MILLI
        ,
    
            TOTAL_LIGHT_SLEEP_TIME_MILLI / 3600000 AS TOTAL_LIGHT_SLEEP_TIME_MILLI
        ,
    
            TOTAL_SLOW_WAVE_SLEEP_TIME_MILLI / 3600000 AS TOTAL_SLOW_WAVE_SLEEP_TIME_MILLI
        ,
    
            TOTAL_REM_SLEEP_TIME_MILLI / 3600000 AS TOTAL_REM_SLEEP_TIME_MILLI
        ,
    
            SLEEP_CYCLE_COUNT::int AS SLEEP_CYCLE_COUNT
        ,
    
            DISTURBANCE_COUNT::int AS DISTURBANCE_COUNT
        ,
    
            BASELINE_MILLI / 3600000 AS BASELINE_MILLI
        ,
    
            NEED_FROM_SLEEP_DEBT_MILLI / 3600000 AS NEED_FROM_SLEEP_DEBT_MILLI
        ,
    
            NEED_FROM_RECENT_STRAIN_MILLI / 3600000 AS NEED_FROM_RECENT_STRAIN_MILLI
        ,
    
            NEED_FROM_RECENT_NAP_MILLI / 3600000 AS NEED_FROM_RECENT_NAP_MILLI
        ,
    
            RESPIRATORY_RATE::int AS RESPIRATORY_RATE
        ,
    
            SLEEP_PERFORMANCE_PERCENTAGE::int AS SLEEP_PERFORMANCE_PERCENTAGE
        ,
    
            SLEEP_CONSISTENCY_PERCENTAGE::int AS SLEEP_CONSISTENCY_PERCENTAGE
        ,
    
            SLEEP_EFFICIENCY_PERCENTAGE::int AS SLEEP_EFFICIENCY_PERCENTAGE
        ,
    
            INGESTED_AT::timestamp_ntz AS INGESTED_AT
        
    
    FROM whoop.raw.sleep

    
    
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM whoop.raw.stg_sleep)
    
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ingested_at DESC, UPDATED_AT DESC) as rn
    FROM ranked_data
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1