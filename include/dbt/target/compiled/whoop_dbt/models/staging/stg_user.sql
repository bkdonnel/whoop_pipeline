








    SELECT
    
            USER_ID::string AS USER_ID
        ,
    
            EMAIL::string AS EMAIL
        ,
    
            FIRST_NAME::string AS FIRST_NAME
        ,
    
            LAST_NAME::string AS LAST_NAME
        ,
    
            CREATED_AT::timestamp_ntz AS CREATED_AT
        ,
    
            UPDATED_AT::timestamp_ntz AS UPDATED_AT
        ,
    
            INGESTED_AT::timestamp_ntz AS INGESTED_AT
        
    
    FROM whoop.raw.user
