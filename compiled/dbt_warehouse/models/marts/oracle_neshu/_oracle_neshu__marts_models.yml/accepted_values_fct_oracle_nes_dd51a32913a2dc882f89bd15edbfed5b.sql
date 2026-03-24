
    
    

with all_values as (

    select
        status_inter as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__machines_maintenance_tracking`
    group by status_inter

)

select *
from all_values
where value_field not in (
    'Ouvert','Planifie','Aucune'
)


