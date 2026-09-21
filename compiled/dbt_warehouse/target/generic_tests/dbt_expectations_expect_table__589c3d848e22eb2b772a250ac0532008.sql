



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 3000 and count(*) <= 20000
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`dim_technique__product`
    

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





