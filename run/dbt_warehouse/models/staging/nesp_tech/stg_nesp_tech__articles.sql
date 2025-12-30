
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles`
      
    partition by date_intervention
    

    
    OPTIONS(
      description="""Mod\u00e8le de staging nettoyant les donn\u00e9es des articles consomm\u00e9s lors des interventions Nespresso issues de la table `nespresso_technique_articles`. Convertit les identifiants en entiers, harmonise les dates, et cast la quantit\u00e9 en float.\n"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nespresso_technique_articles`
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
            else PARSE_DATE('%d/%m/%Y', date)
        end as date_intervention,

        -- Metadata
        timestamp(extracted_at) as extracted_at,
        source_file
        
    from source_data
),

deduped as (
    select *
    from (
        select *,
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
    );
  