
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        entity_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
    group by entity_type

)

select *
from all_values
where value_field not in (
    'company','resource'
)



  
  
      
    ) dbt_internal_test