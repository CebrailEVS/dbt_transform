






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and nb_utilisateurs_distincts >= 0
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_bi__usage_rapport`
    

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







