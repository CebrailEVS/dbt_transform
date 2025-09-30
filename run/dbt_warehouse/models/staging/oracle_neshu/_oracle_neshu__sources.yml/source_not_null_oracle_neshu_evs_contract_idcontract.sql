
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcontract
from `evs-datastack-prod`.`prod_raw`.`evs_contract`
where idcontract is null



  
  
      
    ) dbt_internal_test