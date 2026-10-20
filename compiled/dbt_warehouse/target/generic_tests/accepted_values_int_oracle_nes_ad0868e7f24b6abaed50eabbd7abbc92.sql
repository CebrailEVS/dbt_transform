
    
    

with all_values as (

    select
        task_status_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__task_historic_metier`
    group by task_status_code

)

select *
from all_values
where value_field not in (
    'PREVU','FAIT','ENCOURS','ANNULE','VALIDE','ANOMALIE','ENATTENTE','ACQUITTE'
)


