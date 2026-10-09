
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select bucket_label
from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__sla_buckets`
where bucket_label is null



  
  
      
    ) dbt_internal_test