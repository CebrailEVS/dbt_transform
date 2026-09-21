






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and intervention_date >= date('2023-01-01') and intervention_date <= current_date()
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_technique__repair`
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







