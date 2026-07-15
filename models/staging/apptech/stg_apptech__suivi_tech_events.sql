{{
    config(
        materialized='table',
        description="Événements techniciens (congés, maladie, astreinte…) saisis dans l'app suivi technique — référentiel distinct des interventions"
    )
}}

with source_data as (

    select
        evt_id,
        evt_tech_yuman_id,
        evt_tech_name,
        evt_tech_secteur,
        evt_event_type,
        evt_event_unit,
        evt_event_valeur,
        evt_event_date,
        evt_event_manager_comment,
        annee,
        mois,
        _file_name as gcs_uri
    from {{ source('apptech', 'ext_gcs_apptech__suivi_tech_events') }}

),

cleaned_data as (

    select
        evt_id,
        evt_tech_yuman_id,
        evt_tech_name,
        lower(trim(evt_tech_secteur)) as evt_tech_secteur,
        evt_event_type,
        evt_event_unit,
        evt_event_valeur,
        safe.parse_date('%F', evt_event_date) as evt_event_date,
        nullif(trim(evt_event_manager_comment), '') as evt_event_manager_comment,
        annee,
        mois,

        -- Métadonnées dérivées du chemin GCS ingestion/events/<YYYY-MM>/<timestampZ>.ndjson
        regexp_extract(gcs_uri, r'/ingestion/events/(\d{4}-\d{2})/') as periode,
        regexp_extract(gcs_uri, r'([^/]+\.ndjson)$') as source_file,
        safe.parse_timestamp(
            '%Y%m%dT%H%M%SZ', regexp_extract(gcs_uri, r'/([^/.]+)\.ndjson$')
        ) as extracted_at

    from source_data

)

-- Sémantique SNAPSHOT : seul le dernier fichier de chaque mois fait foi
-- (cf. stg_apptech__suivi_tech_pause pour le détail du contrat).
select
    evt_id,
    evt_tech_yuman_id,
    evt_tech_name,
    evt_tech_secteur,
    evt_event_type,
    evt_event_unit,
    evt_event_valeur,
    evt_event_date,
    evt_event_manager_comment,
    annee,
    mois,
    periode,
    parse_date('%Y-%m', periode) as periode_date,
    source_file,
    extracted_at
from cleaned_data
qualify extracted_at = max(extracted_at) over (partition by periode)
