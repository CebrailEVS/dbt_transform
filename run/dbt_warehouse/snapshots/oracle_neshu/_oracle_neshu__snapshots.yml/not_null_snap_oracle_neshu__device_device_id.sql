
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_id
from `evs-datastack-prod`.`snapshots`.`snap_oracle_neshu__device`
where device_id is null



  
  
      
    ) dbt_internal_test