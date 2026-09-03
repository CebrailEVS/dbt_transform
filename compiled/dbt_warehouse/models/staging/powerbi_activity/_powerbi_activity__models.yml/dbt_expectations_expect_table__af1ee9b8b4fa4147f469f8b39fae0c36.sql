



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 500 and count(*) <= 200000
)
 as expression


    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
    

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





