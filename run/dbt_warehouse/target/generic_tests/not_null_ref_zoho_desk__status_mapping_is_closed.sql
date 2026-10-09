
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_closed
from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__status_mapping`
where is_closed is null



  
  
      
    ) dbt_internal_test