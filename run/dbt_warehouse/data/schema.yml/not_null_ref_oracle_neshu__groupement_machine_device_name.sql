
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_name
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__groupement_machine`
where device_name is null



  
  
      
    ) dbt_internal_test