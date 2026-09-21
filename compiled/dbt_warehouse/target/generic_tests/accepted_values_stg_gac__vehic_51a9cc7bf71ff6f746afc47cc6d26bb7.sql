
    
    

with all_values as (

    select
        dernier_evenement_disponibilite_du_vehicule as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_gac__vehicule`
    group by dernier_evenement_disponibilite_du_vehicule

)

select *
from all_values
where value_field not in (
    'Disponible','Affecté','Indisponible','En maintenance'
)


