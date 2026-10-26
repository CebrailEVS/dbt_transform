
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        workorder_status as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    group by workorder_status

)

select *
from all_values
where value_field not in (
    'Scheduled','In progress','Closed'
)



  
  
      
    ) dbt_internal_test