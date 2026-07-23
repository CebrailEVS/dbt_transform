{{
    config(
        materialized='table',
        description="Délais forcés (technicien/partenaire) par intervention curative, depuis les snapshots NDJSON de l'app suivi technique"
    )
}}

with source_data as (

    select
        intervention_id,
        src_inter,
        numero_pu,
        delai_tech_force,
        delai_partenaire_force,
        commentaire,
        _file_name as gcs_uri
    from {{ source('apptech', 'ext_gcs_apptech__suivi_tech_curative') }}

),

cleaned_data as (

    select
        intervention_id,
        upper(trim(src_inter)) as src_inter,
        nullif(trim(numero_pu), '') as numero_pu,
        upper(trim(delai_tech_force)) as delai_tech_force,
        upper(trim(delai_partenaire_force)) as delai_partenaire_force,
        nullif(trim(commentaire), '') as commentaire,

        -- Métadonnées dérivées du chemin GCS ingestion/curative/<YYYY-MM>/<timestampZ>.ndjson
        regexp_extract(gcs_uri, r'/ingestion/curative/(\d{4}-\d{2})/') as periode,
        regexp_extract(gcs_uri, r'([^/]+\.ndjson)$') as source_file,
        safe.parse_timestamp(
            '%Y%m%dT%H%M%SZ', regexp_extract(gcs_uri, r'/([^/.]+)\.ndjson$')
        ) as extracted_at

    from source_data

)

-- Sémantique SNAPSHOT : seul le dernier fichier de chaque mois fait foi
-- (cf. stg_apptech__suivi_tech_pause pour le détail du contrat).
select
    intervention_id,
    src_inter,
    numero_pu,
    delai_tech_force,
    delai_partenaire_force,
    commentaire,
    periode,
    parse_date('%Y-%m', periode) as periode_date,
    source_file,
    extracted_at
from cleaned_data
qualify extracted_at = max(extracted_at) over (partition by periode)
