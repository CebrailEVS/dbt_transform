
    
    

with all_values as (

    select
        status_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__pointage_tasks`
    group by status_code

)

select *
from all_values
where value_field not in (
    'VALIDE','FAIT','ANNULE','ANOMALIE','PREVU','ENCOURS','ACQUITTE','ENATTENTE'
)


