



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 1000 and count(*) <= 100000
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_commerce__opportunite`
    

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





