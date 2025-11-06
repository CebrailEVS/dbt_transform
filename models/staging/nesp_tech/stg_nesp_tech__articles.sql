{{
    config(
        materialized='table',
        partition_by={
            "field": "date_intervention",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['n_planning'],
        description='Articles consommées nettoyée depuis nespresso_technique_articles'
    )
}}

with source_data as (
    select *
    from {{ source('nesp_tech', 'nespresso_technique_articles') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT (avec traitement des nan)
        case 
            when lower(trim(cast(n_planning as string))) = 'nan' then null
            else cast(n_planning as int64)
        end as n_planning,
        case 
            when lower(trim(cast(n_client as string))) = 'nan' then null
            else cast(n_client as int64)
        end as n_client,
        case 
            when lower(trim(cast(n_site as string))) = 'nan' then null
            else CAST(CAST(n_site AS FLOAT64) AS INT64)
        end as n_site,

        -- Colonnes texte (avec traitement "nan" -> NULL)
        nullif(lower(trim(n_tech)), 'nan') as n_tech,
        nullif(lower(trim(nom_tech)), 'nan') as nom_tech,
        nullif(lower(trim(pr_nom_tech)), 'nan') as prenom_tech,
        nullif(lower(trim(raison_sociale_client)), 'nan') as raison_sociale_client,
        nullif(lower(trim(nom_site)), 'nan') as nom_site,
        nullif(lower(trim(code_machine)), 'nan') as code_machine,
        nullif(lower(trim(nom_machine)), 'nan') as nom_machine,
        nullif(lower(trim(n_s_rie_machine)), 'nan') as num_serie_machine,
        nullif(lower(trim(code_article)), 'nan') as code_article,
        nullif(lower(trim(nom_article)), 'nan') as nom_article,

        -- Mesure
        cast(quantit as float64) as quantite_article,

        -- Dates harmonisées (converties en DATES, avec traitement "NaT" -> NULL)
        case 
            when lower(trim(date)) in ('nat', 'nan') then null
            else PARSE_DATE('%d/%m/%Y', date)
        end as date_intervention,

        -- Metadata
        timestamp(extracted_at) as extracted_at,
        timestamp(file_date) as file_date,
        source_file
        
    from source_data
)

select * from cleaned_data