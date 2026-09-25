
    
    

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


