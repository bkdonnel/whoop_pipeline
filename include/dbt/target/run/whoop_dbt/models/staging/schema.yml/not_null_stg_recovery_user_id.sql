
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from whoop.raw.stg_recovery
where USER_ID is null



  
  
      
    ) dbt_internal_test