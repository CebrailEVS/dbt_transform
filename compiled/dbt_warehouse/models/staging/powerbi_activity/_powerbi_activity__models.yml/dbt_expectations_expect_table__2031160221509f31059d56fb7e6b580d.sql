



    with grouped_expression as (
    select
        
        
    
  
( 1=1 and count(*) >= 5 and count(*) <= 40
)
 as expression


    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__workspaces`
    

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





