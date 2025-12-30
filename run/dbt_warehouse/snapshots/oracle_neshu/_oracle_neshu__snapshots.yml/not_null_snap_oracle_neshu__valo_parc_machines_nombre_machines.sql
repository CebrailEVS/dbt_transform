
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nombre_machines
from `evs-datastack-prod`.`snapshots`.`snap_oracle_neshu__valo_parc_machines`
where nombre_machines is null



  
  
      
    ) dbt_internal_test