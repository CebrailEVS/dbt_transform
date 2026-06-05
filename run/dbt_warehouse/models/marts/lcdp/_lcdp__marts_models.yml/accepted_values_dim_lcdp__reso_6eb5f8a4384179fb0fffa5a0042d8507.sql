
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        resources_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource`
    group by resources_type

)

select *
from all_values
where value_field not in (
    'PERSON','VEHICLE'
)



  
  
      
    ) dbt_internal_test