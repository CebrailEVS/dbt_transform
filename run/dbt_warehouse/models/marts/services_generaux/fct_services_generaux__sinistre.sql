
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_services_generaux__sinistre`
      
    
    

    
    OPTIONS(
      description="""Sinistres v\u00e9hicules issus du fournisseur GAC, enrichis pour reporting Services G\u00e9n\u00e9raux."""
    )
    as (
      

select
    -- Infos Sinistre
    n_de_sinistre,
    date(date_sinistre) as date_sinistre,
    date_de_creation,
    circonstance,
    tiers,
    resp,
    cloture,

    -- Infos Vehicule
    immat,
    nom,
    prenom,
    statut_actuel,
    genre_fiscal,
    reference_gac,
    entite_entite_2,
    entite_entite_3,

    -- Couts
    centre_de_couts,
    cout_assureur,
    auto_assurance,
    franchise,
    cout_global,
    cout_client,

    -- Metadonnees dbt
    current_timestamp() as dbt_updated_at,
    'd152e8fe-9871-4c4a-ac59-fd7728265ab3' as dbt_invocation_id

from `evs-datastack-prod`.`prod_staging`.`stg_gac__sinistres_sg`
    );
  