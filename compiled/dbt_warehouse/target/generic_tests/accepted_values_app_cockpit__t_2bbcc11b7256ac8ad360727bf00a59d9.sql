
    
    

with all_values as (

    select
        atelier_destination as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_commande_fournisseur`
    group by atelier_destination

)

select *
from all_values
where value_field not in (
    '06 - ATELIER RUNGIS DEPOT','07 - ATELIER LYON DEPOT','08 - ATELIER BORDEAUX DEPOT','09 - ATELIER STRASBOURG DEPOT'
)


