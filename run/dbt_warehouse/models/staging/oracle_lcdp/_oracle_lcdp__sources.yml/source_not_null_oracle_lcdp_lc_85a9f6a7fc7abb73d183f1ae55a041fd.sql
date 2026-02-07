
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idlocation
from `evs-datastack-prod`.`prod_raw`.`lcdp_company_has_location`
where idlocation is null



  
  
      
    ) dbt_internal_test