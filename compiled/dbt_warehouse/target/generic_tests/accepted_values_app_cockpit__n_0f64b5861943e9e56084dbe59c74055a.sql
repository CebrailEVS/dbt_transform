
    
    

with all_values as (

    select
        motif_exclusion as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_commande_delai`
    group by motif_exclusion

)

select *
from all_values
where value_field not in (
    'ANNULEE','HORS_CHARGEMENT_VEHICULE','SANS_ARTICLE_STOCKE','FACTUREE_SANS_BL'
)


