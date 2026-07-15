

with source_data as (

    select
        intervention_id,
        a_facturer,
        tech_id_reel,
        tech_yuman_id_reel,
        commentaire,
        _file_name as gcs_uri
    from `evs-datastack-prod`.`prod_raw`.`ext_gcs_apptech__suivi_tech_modif_intervention`

),

cleaned_data as (

    select
        intervention_id,
        -- a_facturer peut être vide dans la source (modif sans décision de facturation)
        upper(nullif(trim(a_facturer), '')) as a_facturer,
        lower(trim(tech_id_reel)) as tech_id_reel,
        tech_yuman_id_reel,
        nullif(trim(commentaire), '') as commentaire,

        -- Métadonnées dérivées du chemin GCS ingestion/modif_intervention/<YYYY-MM>/<timestampZ>.ndjson
        regexp_extract(gcs_uri, r'/ingestion/modif_intervention/(\d{4}-\d{2})/') as periode,
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
    a_facturer,
    tech_id_reel,
    tech_yuman_id_reel,
    commentaire,
    periode,
    parse_date('%Y-%m', periode) as periode_date,
    source_file,
    extracted_at
from cleaned_data
qualify extracted_at = max(extracted_at) over (partition by periode)