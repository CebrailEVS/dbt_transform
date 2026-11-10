
    
    

with all_values as (

    select
        movement_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_mouvement_machine`
    group by movement_type

)

select *
from all_values
where value_field not in (
    'Installation','Vente','Retour','Retour en casse','Transfert entre dépôts','Mise en casse','Réception','Transfert entre clients','Création de la fiche','Sous-équipement monté','Sous-équipement démonté','Autre'
)


