
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__source_freshness`
    group by status

)

select *
from all_values
where value_field not in (
    'pass','warn','error','runtime error'
)



  
  
      
    ) dbt_internal_test