
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select device_id
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__machines_yuman_maintenance_base`
where device_id is null



  
  
      
    ) dbt_internal_test