
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        demand_status as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    group by demand_status

)

select *
from all_values
where value_field not in (
    'Open','Accepted','Rejected','Closed'
)



  
  
      
    ) dbt_internal_test