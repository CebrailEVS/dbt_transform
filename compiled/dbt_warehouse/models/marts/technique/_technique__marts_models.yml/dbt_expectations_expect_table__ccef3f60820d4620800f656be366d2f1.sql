



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 10000 and count(*) <= 200000
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_technique__workorder_product`
    

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





