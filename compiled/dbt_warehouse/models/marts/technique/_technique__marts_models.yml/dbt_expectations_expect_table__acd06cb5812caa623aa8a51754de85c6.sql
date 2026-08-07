



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 1 and count(*) <= 2000
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
    

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





