
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcompany
from `evs-datastack-prod`.`prod_raw`.`evs_company_has_location`
where idcompany is null



  
  
      
    ) dbt_internal_test