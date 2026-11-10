
    
    

with all_values as (

    select
        type_tache as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_livraison_interne`
    group by type_tache

)

select *
from all_values
where value_field not in (
    'LIVRAISON_INTERNE','SORTIE_FABRICATION','ENTREE_FABRICATION'
)


