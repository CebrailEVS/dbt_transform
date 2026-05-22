
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_services_generaux__sinistre`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Sinistres de la flotte de v\u00e9hicules EVS (assurance, responsabilit\u00e9, co\u00fbts).\n[COMMENT CONSTRUITE] Lecture directe de stg_gac__sinistres_sg (fichier CSV d\u00e9pos\u00e9 par GAC sur SFTP), avec extraction de la date de sinistre depuis le timestamp source et ajout des m\u00e9tadonn\u00e9es dbt. Pas d'enrichissement m\u00e9tier ni de jointure.\n[GRAIN] 1 ligne par sinistre (n_de_sinistre).\n[NOTES] Source GAC = gestionnaire de flotte externe. Co\u00fbts ventil\u00e9s en : cout_assureur, auto_assurance, franchise, cout_global, cout_client.\n"""
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
    'c4f6f006-9720-4500-a4b4-44785657db4c' as dbt_invocation_id

from `evs-datastack-prod`.`prod_staging`.`stg_gac__sinistres_sg`
    );
  