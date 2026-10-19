
    
    

with all_values as (

    select
        categorie as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_equipement_stock`
    group by categorie

)

select *
from all_values
where value_field not in (
    'Machine principale','Machine (comptoir ou à monter)','Composant en stock','Composant rattaché','Terminal de paiement','À classer'
)


