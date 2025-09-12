







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
    
            SPORT_NAME::string AS SPORT_NAME
        ,
    
            SPORT_ID::string AS SPORT_ID
        ,
    
            SCORE_STATE::string AS SCORE_STATE
        ,
    
            STRAIN::int AS STRAIN
        ,
    
            AVERAGE_HEART_RATE::int AS AVERAGE_HEART_RATE
        ,
    
            MAX_HEART_RATE::int AS MAX_HEART_RATE
        ,
    
            KILOJOULE::int AS KILOJOULE
        ,
    
            PERCENT_RECORDED::int AS PERCENT_RECORDED
        ,
    
            DISTANCE_METER::int AS DISTANCE_METER
        ,
    
            ALTITUDE_GAIN_METER::int AS ALTITUDE_GAIN_METER
        ,
    
            ALTITUDE_CHANGE_METER::int AS ALTITUDE_CHANGE_METER
        ,
    
            ZONE_ZERO_MILLI / 60000 AS ZONE_ZERO_MILLI
        ,
    
            ZONE_ONE_MILLI / 60000 AS ZONE_ONE_MILLI
        ,
    
            ZONE_TWO_MILLI / 60000 AS ZONE_TWO_MILLI
        ,
    
            ZONE_THREE_MILLI / 60000 AS ZONE_THREE_MILLI
        ,
    
            ZONE_FOUR_MILLI / 60000 AS ZONE_FOUR_MILLI
        ,
    
            ZONE_FIVE_MILLI / 60000 AS ZONE_FIVE_MILLI
        ,
    
            INGESTED_AT::timestamp_ntz AS INGESTED_AT
        
    
    FROM whoop.raw.workout

    
    
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM whoop.raw.stg_workout)
    
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ingested_at DESC, UPDATED_AT DESC) as rn
    FROM ranked_data
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1