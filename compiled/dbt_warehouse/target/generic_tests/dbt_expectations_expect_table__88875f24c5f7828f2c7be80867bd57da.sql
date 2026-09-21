



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 100000 and count(*) <= 1000000
)
 as expression


    from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
    

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





