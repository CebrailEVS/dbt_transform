
    
    

with all_values as (

    select
        sens as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_mouvement_stock`
    group by sens

)

select *
from all_values
where value_field not in (
    'entree','sortie','aucun'
)


