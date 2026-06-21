
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        task_status_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__inter_technique_tasks`
    group by task_status_code

)

select *
from all_values
where value_field not in (
    'FAIT','ANNULE','PREVU','ENCOURS','ANOMALIE','VALIDE','ACQUITTE','ENATTENTE'
)



  
  
      
    ) dbt_internal_test