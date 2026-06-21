
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        telemetry_source as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__telemetry_tasks`
    group by telemetry_source

)

select *
from all_values
where value_field not in (
    'TELEM_NAYAX'
)



  
  
      
    ) dbt_internal_test