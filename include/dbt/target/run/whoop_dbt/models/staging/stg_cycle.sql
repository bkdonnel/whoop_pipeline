-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into whoop.raw.stg_cycle as DBT_INTERNAL_DEST
        using whoop.raw.stg_cycle__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.ID = DBT_INTERNAL_DEST.ID
            )

    
    when matched then update set
        UPDATED_AT = DBT_INTERNAL_SOURCE.UPDATED_AT,INGESTED_AT = DBT_INTERNAL_SOURCE.INGESTED_AT
    

    when not matched then insert
        ("ID", "USER_ID", "CREATED_AT", "UPDATED_AT", "START_TIME", "END_TIME", "TIMEZONE_OFFSET", "SCORE_STATE", "STRAIN", "KILOJOULE", "AVERAGE_HEART_RATE", "MAX_HEART_RATE", "INGESTED_AT")
    values
        ("ID", "USER_ID", "CREATED_AT", "UPDATED_AT", "START_TIME", "END_TIME", "TIMEZONE_OFFSET", "SCORE_STATE", "STRAIN", "KILOJOULE", "AVERAGE_HEART_RATE", "MAX_HEART_RATE", "INGESTED_AT")

;
    commit;