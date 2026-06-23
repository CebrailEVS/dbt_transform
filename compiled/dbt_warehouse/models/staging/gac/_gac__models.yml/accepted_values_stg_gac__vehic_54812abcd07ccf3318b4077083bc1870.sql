
    
    

with all_values as (

    select
        contrat_type_etat as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_gac__vehicule`
    group by contrat_type_etat

)

select *
from all_values
where value_field not in (
    'Actif','Inactif'
)


