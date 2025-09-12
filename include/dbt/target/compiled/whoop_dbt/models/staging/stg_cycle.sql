







WITH ranked_data AS (
    
    SELECT
    
            ID::string AS ID
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
    
            SCORE_STATE::string AS SCORE_STATE
        ,
    
            STRAIN::int AS STRAIN
        ,
    
            KILOJOULE::int AS KILOJOULE
        ,
    
            AVERAGE_HEART_RATE::int AS AVERAGE_HEART_RATE
        ,
    
            MAX_HEART_RATE::int AS MAX_HEART_RATE
        ,
    
            INGESTED_AT::timestamp_ntz AS INGESTED_AT
        
    
    FROM whoop.raw.cycle

    
    
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM whoop.raw.stg_cycle)
    
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ingested_at DESC, UPDATED_AT DESC) as rn
    FROM ranked_data
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1