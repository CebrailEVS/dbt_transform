
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        client_category as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    group by client_category

)

select *
from all_values
where value_field not in (
    'PREMIUM','VIP','OR','ARGENT','BRONZE'
)



  
  
      
    ) dbt_internal_test