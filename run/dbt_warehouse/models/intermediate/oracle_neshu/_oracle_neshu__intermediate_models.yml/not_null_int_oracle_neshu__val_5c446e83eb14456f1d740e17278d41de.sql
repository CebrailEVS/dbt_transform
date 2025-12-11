
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_group
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__valorisation_parc_machines`
where device_group is null



  
  
      
    ) dbt_internal_test