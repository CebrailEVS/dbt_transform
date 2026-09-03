
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        format as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__reports`
    group by format

)

select *
from all_values
where value_field not in (
    'PBIR','PBIRLegacy'
)



  
  
      
    ) dbt_internal_test