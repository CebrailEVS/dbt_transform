






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and delai_jours_depuis_precedente >= 0
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







