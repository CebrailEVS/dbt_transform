
    
    

with all_values as (

    select
        statut as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nayka_equipement_stock`
    group by statut

)

select *
from all_values
where value_field not in (
    'Disponible','Réservé','À remettre en état','Inactif hors rebut (à investiguer)','Rebut','Commandé','À solder','Hors stock (marketing)'
)


