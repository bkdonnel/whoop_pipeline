
    
    

select
    id as unique_field,
    count(*) as n_records

from whoop.raw.stg_sleep
where id is not null
group by id
having count(*) > 1


