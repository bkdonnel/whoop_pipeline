







WITH base AS (
    
    SELECT
    
            CYCLE_ID::string AS CYCLE_ID
        ,
    
            SLEEP_ID::string AS SLEEP_ID
        ,
    
            USER_ID::string AS USER_ID
        ,
    
            CREATED_AT::timestamp_ntz AS CREATED_AT
        ,
    
            UPDATED_AT::timestamp_ntz AS UPDATED_AT
        ,
    
            SCORE_STATE::string AS SCORE_STATE
        ,
    
            USER_CALIBRATING::boolean AS USER_CALIBRATING
        ,
    
            RECOVERY_SCORE::int AS RECOVERY_SCORE
        ,
    
            RESTING_HEART_RATE::int AS RESTING_HEART_RATE
        ,
    
            HRV_RMSSD_MILLI::int AS HRV_RMSSD_MILLI
        ,
    
            SPO2_PERCENTAGE::int AS SPO2_PERCENTAGE
        ,
    
            SKIN_TEMP_CELSIUS::int AS SKIN_TEMP_CELSIUS
        ,
    
            INGESTED_AT::timestamp_ntz AS INGESTED_AT
        
    
    FROM whoop.raw.recovery

    
    
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM whoop.raw.stg_recovery)
    
)

SELECT * FROM base