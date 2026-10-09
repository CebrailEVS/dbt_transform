
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lower_min
from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__sla_buckets`
where lower_min is null



  
  
      
    ) dbt_internal_test