
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        workorder_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__workorder_product`
    group by workorder_type

)

select *
from all_values
where value_field not in (
    'Reactive','Preventive','Installation'
)



  
  
      
    ) dbt_internal_test