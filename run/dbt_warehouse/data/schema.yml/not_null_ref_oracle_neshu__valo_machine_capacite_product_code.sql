
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_code
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__valo_machine_capacite`
where product_code is null



  
  
      
    ) dbt_internal_test