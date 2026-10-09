
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        activite_date, report_id
    from `evs-datastack-prod`.`prod_marts`.`fct_bi__activite_rapport_jour`
    group by activite_date, report_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test