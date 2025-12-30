
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valorisation_totale_machine
from `evs-datastack-prod`.`snapshots`.`snap_oracle_neshu__valo_parc_machines`
where valorisation_totale_machine is null



  
  
      
    ) dbt_internal_test