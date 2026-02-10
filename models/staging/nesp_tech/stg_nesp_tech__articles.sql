{{
    config(
        materialized = 'table',
        partition_by = {
            "field": "date_intervention",
            "data_type": "date",
            "granularity": "day"
        },
        description = 'Articles consommées nettoyée depuis nespresso_technique_articles'
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
            else cast(cast(n_site as float64) as int64)
        end as n_site,

        -- Colonnes texte
        nullif(lower(trim(n_tech)), 'nan') as n_tech,
        nullif(lower(trim(nom_tech)), 'nan') as nom_tech,
        nullif(lower(trim(prenom_tech)), 'nan') as prenom_tech,
        nullif(lower(trim(raison_sociale_client)), 'nan') as raison_sociale_client,
        nullif(lower(trim(nom_site)), 'nan') as nom_site,
        nullif(lower(trim(code_machine)), 'nan') as code_machine,
        nullif(lower(trim(nom_machine)), 'nan') as nom_machine,
        nullif(lower(trim(n_serie_machine)), 'nan') as num_serie_machine,
        nullif(lower(trim(code_article)), 'nan') as code_article,
        nullif(lower(trim(nom_article)), 'nan') as nom_article,

        -- Mesure
        cast(quantite as float64) as quantite_article,

        -- Dates harmonisées
        case
            when lower(trim(date)) in ('nat', 'nan') then null
            else parse_date('%d/%m/%Y', date)
        end as date_intervention,

        -- Metadata
        timestamp(extracted_at) as extracted_at,
        source_file

    from source_data

),

deduped as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by n_planning, code_article, date_intervention
                order by extracted_at desc
            ) as rn
        from cleaned_data
    )
    where rn = 1

)

select *
from deduped
