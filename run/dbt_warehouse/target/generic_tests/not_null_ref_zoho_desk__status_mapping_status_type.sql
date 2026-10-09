
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status_type
from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__status_mapping`
where status_type is null



  
  
      
    ) dbt_internal_test