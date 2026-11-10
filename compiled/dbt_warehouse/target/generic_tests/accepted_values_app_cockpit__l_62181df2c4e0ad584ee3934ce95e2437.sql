
    
    

with all_values as (

    select
        task_status_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_mouvement_machine`
    group by task_status_code

)

select *
from all_values
where value_field not in (
    'FAIT','VALIDE','ANNULE','ANOMALIE'
)


