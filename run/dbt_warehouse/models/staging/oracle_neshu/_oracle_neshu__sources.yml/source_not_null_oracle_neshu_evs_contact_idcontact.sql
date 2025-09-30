
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcontact
from `evs-datastack-prod`.`prod_raw`.`evs_contact`
where idcontact is null



  
  
      
    ) dbt_internal_test