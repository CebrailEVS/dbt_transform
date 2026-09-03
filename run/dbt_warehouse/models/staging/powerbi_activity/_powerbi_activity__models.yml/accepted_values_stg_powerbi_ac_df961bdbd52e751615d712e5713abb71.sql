
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        report_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__reports`
    group by report_type

)

select *
from all_values
where value_field not in (
    'PowerBIReport','PaginatedReport'
)



  
  
      
    ) dbt_internal_test