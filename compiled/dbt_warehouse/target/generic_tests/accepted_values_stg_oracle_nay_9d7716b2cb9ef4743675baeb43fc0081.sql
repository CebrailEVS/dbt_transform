
    
    

with all_values as (

    select
        code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_status`
    group by code

)

select *
from all_values
where value_field not in (
    'PREVU','FAIT','ENCOURS','ANNULE','VALIDE','ANOMALIE','ENATTENTE','ACQUITTE'
)


