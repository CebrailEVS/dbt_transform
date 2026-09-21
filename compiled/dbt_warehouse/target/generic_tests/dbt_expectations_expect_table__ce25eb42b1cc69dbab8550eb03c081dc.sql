



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 20 and count(*) <= 500000
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_bi__consultation`
    

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





