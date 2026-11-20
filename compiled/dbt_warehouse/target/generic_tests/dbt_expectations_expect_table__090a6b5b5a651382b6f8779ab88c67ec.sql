



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 100 and count(*) <= 5000
)
 as expression


    from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_alerte_conso_aguila`
    

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





