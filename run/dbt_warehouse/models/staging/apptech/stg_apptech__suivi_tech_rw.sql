
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_rw`
      
    
    

    
    OPTIONS(
      description="""Re-mappings d'interventions (rework) saisis dans l'app suivi technique : une ligne = un id d'intervention erron\u00e9 r\u00e9affect\u00e9 vers le bon id, avec le technicien concern\u00e9. Dedup snapshot : seules les lignes du dernier fichier NDJSON de chaque mois sont conserv\u00e9es. Pas de test de freshness : soumissions humaines sans cadence garantie.\n"""
    )
    as (
      

with source_data as (

    select
        bad_intervention_id,
        new_intervention_id,
        tech_id,
        tech_nom,
        tech_secteur,
        annee,
        mois,
        source,
        _file_name as gcs_uri
    from `evs-datastack-prod`.`prod_raw`.`ext_gcs_apptech__suivi_tech_rw`

),

cleaned_data as (

    select
        bad_intervention_id,
        new_intervention_id,
        tech_id,
        tech_nom,
        lower(trim(tech_secteur)) as tech_secteur,
        annee,
        mois,
        source,

        -- Métadonnées dérivées du chemin GCS ingestion/rw/<YYYY-MM>/<timestampZ>.ndjson
        regexp_extract(gcs_uri, r'/ingestion/rw/(\d{4}-\d{2})/') as periode,
        regexp_extract(gcs_uri, r'([^/]+\.ndjson)$') as source_file,
        safe.parse_timestamp(
            '%Y%m%dT%H%M%SZ', regexp_extract(gcs_uri, r'/([^/.]+)\.ndjson$')
        ) as extracted_at

    from source_data

)

-- Sémantique SNAPSHOT : seul le dernier fichier de chaque mois fait foi
-- (cf. stg_apptech__suivi_tech_pause pour le détail du contrat).
select
    bad_intervention_id,
    new_intervention_id,
    tech_id,
    tech_nom,
    tech_secteur,
    annee,
    mois,
    source,
    periode,
    parse_date('%Y-%m', periode) as periode_date,
    source_file,
    extracted_at
from cleaned_data
qualify extracted_at = max(extracted_at) over (partition by periode)
    );
  