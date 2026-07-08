



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 10000 and count(*) <= 2000000
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
    

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





