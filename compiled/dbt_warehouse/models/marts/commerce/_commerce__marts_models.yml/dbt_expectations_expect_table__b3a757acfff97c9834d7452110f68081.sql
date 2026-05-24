



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 100 and count(*) <= 100000
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_commerce__machine_intervention`
    

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





