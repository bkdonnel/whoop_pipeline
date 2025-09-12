
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select user_id
from whoop.raw.stg_cycle
where user_id is null



  
  
      
    ) dbt_internal_test