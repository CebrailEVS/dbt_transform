
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        task_status_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__appel_sav`
    group by task_status_code

)

select *
from all_values
where value_field not in (
    'VALIDE','PREVU'
)



  
  
      
    ) dbt_internal_test