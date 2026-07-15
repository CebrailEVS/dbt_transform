
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_pause`
      
    
    

    
    OPTIONS(
      description="""Overrides de prime \"pause\" saisis dans l'app suivi technique : une ligne = une intervention dont la prime doit \u00eatre doubl\u00e9e (ou non). Dedup snapshot : seules les lignes du dernier fichier NDJSON de chaque mois sont conserv\u00e9es (une ligne absente du dernier snapshot = override r\u00e9voqu\u00e9). Pas de test de freshness : soumissions humaines sans cadence garantie. La jointure vers le r\u00e9f\u00e9rentiel d'interventions est g\u00e9r\u00e9e en aval (intermediate).\n"""
    )
    as (
      

with source_data as (

    select
        intervention_id,
        doubler_prime,
        commentaire,
        _file_name as gcs_uri
    from `evs-datastack-prod`.`prod_raw`.`ext_gcs_apptech__suivi_tech_pause`

),

cleaned_data as (

    select
        intervention_id,
        upper(trim(doubler_prime)) as doubler_prime,
        nullif(trim(commentaire), '') as commentaire,

        -- Métadonnées dérivées du chemin GCS ingestion/pause/<YYYY-MM>/<timestampZ>.ndjson
        regexp_extract(gcs_uri, r'/ingestion/pause/(\d{4}-\d{2})/') as periode,
        regexp_extract(gcs_uri, r'([^/]+\.ndjson)$') as source_file,
        safe.parse_timestamp(
            '%Y%m%dT%H%M%SZ', regexp_extract(gcs_uri, r'/([^/.]+)\.ndjson$')
        ) as extracted_at

    from source_data

)

-- Sémantique SNAPSHOT : chaque fichier ré-émet l'état complet du mois pour son
-- type ; seul le dernier fichier de chaque mois fait foi (une ligne absente du
-- dernier snapshot = override révoqué). L'historique complet reste dans GCS.
select
    intervention_id,
    doubler_prime,
    commentaire,
    periode,
    parse_date('%Y-%m', periode) as periode_date,
    source_file,
    extracted_at
from cleaned_data
qualify extracted_at = max(extracted_at) over (partition by periode)
    );
  