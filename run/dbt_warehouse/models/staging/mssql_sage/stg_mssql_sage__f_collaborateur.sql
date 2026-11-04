
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_collaborateur`
      
    
    

    
    OPTIONS(
      description="""Collaborateurs du dossier Sage (dbo_f_collaborateur), une ligne par collaborateur (`co_no`) : les commerciaux pour la plupart, et quelques acheteurs. Rattach\u00e9s aux comptes tiers et aux documents par `co_no`."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_collaborateur`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Champs principaux
        co_no,
        co_nom,
        co_prenom,
        co_fonction,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
    );
  