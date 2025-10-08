
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        source_last_preventive as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__machines_maintenance_tracking`
    group by source_last_preventive

)

select *
from all_values
where value_field not in (
    'yuman','dlog','aucune'
)



  
  
      
    ) dbt_internal_test