






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and nb_rafraichissements >= 0
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_bi__activite_rapport_jour`
    

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







