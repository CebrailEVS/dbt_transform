{{
    config(
        materialized='table',
        description="Overrides Aguila (conversion code 5) par intervention, depuis les snapshots NDJSON de l'app suivi technique"
    )
}}

with source_data as (

    select
        intervention_id,
        convertir_code_5,
        commentaire,
        _file_name as gcs_uri
    from {{ source('apptech', 'ext_gcs_apptech__suivi_tech_aguila') }}

),

cleaned_data as (

    select
        intervention_id,
        upper(trim(convertir_code_5)) as convertir_code_5,
        nullif(trim(commentaire), '') as commentaire,

        -- Métadonnées dérivées du chemin GCS ingestion/aguila/<YYYY-MM>/<timestampZ>.ndjson
        regexp_extract(gcs_uri, r'/ingestion/aguila/(\d{4}-\d{2})/') as periode,
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
    convertir_code_5,
    commentaire,
    periode,
    parse_date('%Y-%m', periode) as periode_date,
    source_file,
    extracted_at
from cleaned_data
qualify extracted_at = max(extracted_at) over (partition by periode)
