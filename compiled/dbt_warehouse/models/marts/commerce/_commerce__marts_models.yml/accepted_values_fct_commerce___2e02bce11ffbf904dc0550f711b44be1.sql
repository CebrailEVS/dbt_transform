
    
    

with all_values as (

    select
        act_statut as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_commerce__activite`
    group by act_statut

)

select *
from all_values
where value_field not in (
    'En cours','Terminé'
)


