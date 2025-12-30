
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_group
from `evs-datastack-prod`.`snapshots`.`snap_oracle_neshu__valo_parc_machines`
where device_group is null



  
  
      
    ) dbt_internal_test