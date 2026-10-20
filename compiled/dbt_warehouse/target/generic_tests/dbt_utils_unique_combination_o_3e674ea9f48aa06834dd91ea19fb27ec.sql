





with validation_errors as (

    select
        commande_id
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_commande_delai`
    group by commande_id
    having count(*) > 1

)

select *
from validation_errors


