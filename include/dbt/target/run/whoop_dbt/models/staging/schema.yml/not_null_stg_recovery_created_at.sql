
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CREATED_AT
from whoop.raw.stg_recovery
where CREATED_AT is null



  
  
      
    ) dbt_internal_test