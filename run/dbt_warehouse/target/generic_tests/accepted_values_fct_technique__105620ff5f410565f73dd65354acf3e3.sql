
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        agency as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
    group by agency

)

select *
from all_values
where value_field not in (
    'evs','evs idf','evs paris','evs paris 2'
)



  
  
      
    ) dbt_internal_test