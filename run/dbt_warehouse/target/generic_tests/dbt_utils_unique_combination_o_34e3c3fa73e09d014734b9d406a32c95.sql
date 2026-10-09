
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        metric, bucket_order
    from `evs-datastack-prod`.`prod_reference`.`ref_zoho_desk__sla_buckets`
    group by metric, bucket_order
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test