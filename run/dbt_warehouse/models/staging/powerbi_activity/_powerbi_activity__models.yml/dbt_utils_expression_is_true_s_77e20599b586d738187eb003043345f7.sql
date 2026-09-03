
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__reports`

where not(lower(report_name) not like '%usage metrics report%')


  
  
      
    ) dbt_internal_test