
    
    

with all_values as (

    select
        co_statut as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_co__commerciaux`
    group by co_statut

)

select *
from all_values
where value_field not in (
    'Actif','Inactif'
)


