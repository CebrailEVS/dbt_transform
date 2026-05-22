
    
    

with all_values as (

    select
        status_inter as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__maintenance_preventive`
    group by status_inter

)

select *
from all_values
where value_field not in (
    'Ouvert','Planifie','Aucune'
)


