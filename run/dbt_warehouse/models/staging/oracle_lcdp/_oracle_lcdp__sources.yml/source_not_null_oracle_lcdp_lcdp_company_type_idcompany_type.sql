
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idcompany_type
from `evs-datastack-prod`.`prod_raw`.`lcdp_company_type`
where idcompany_type is null



  
  
      
    ) dbt_internal_test