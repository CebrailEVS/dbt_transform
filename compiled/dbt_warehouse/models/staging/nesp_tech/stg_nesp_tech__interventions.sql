

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nespresso_technique_interventions`
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
        nullif(lower(trim(prenom_tech)), 'nan') as prenom_tech,
        nullif(lower(trim(raison_sociale_client)), 'nan') as raison_sociale_client,
        nullif(lower(trim(adresse_client)), 'nan') as adresse_client,
        nullif(lower(trim(code_postal_client)), 'nan') as code_postal_client,
        nullif(lower(trim(ville_client)), 'nan') as ville_client,
        nullif(lower(trim(nom_site)), 'nan') as nom_site,
        nullif(lower(trim(adresse_site)), 'nan') as adresse_site,
        case
            when regexp_contains(nullif(lower(trim(code_postal_site)), 'nan'), r'^\d{4}$')
                then concat('0', nullif(lower(trim(code_postal_site)), 'nan'))
            else nullif(lower(trim(code_postal_site)), 'nan')
        end as code_postal_site,
        nullif(lower(trim(ville_site)), 'nan') as ville_site,
        nullif(lower(trim(code_machine)), 'nan') as code_machine,
        nullif(lower(trim(nom_machine)), 'nan') as nom_machine,
        nullif(lower(trim(n_serie_machine)), 'nan') as num_serie_machine,
        nullif(lower(trim(type)), 'nan') as type,
        nullif(lower(trim(etat_inter)), 'nan') as etat_intervention,
        nullif(lower(trim(observations)), 'nan') as observations,
        nullif(lower(trim(agency)), 'nan') as agency,
        regexp_replace(
            nullif(lower(trim(repair_code_1)), 'nan'),
            r'\.0$',
            ''
        ) as repair_code_1,
        regexp_replace(
            nullif(lower(trim(repair_code_2)), 'nan'),
            r'\.0$',
            ''
        ) as repair_code_2,

        regexp_replace(
            nullif(lower(trim(repair_code_3)), 'nan'),
            r'\.0$',
            ''
        ) as repair_code_3,
        nullif(lower(trim(failure_code)), 'nan') as failure_code,
        nullif(lower(trim(consignes)), 'nan') as consignes,
        -- Dates harmonisées (converties en TIMESTAMP, avec traitement "NaT" et dates invalides -> NULL)
        case 
            when lower(trim(date_heure_debut)) in ('nat', 'nan') then null
            when date_heure_debut like '01/01/0001%' then null
            else TIMESTAMP(date_heure_debut)
        end as date_heure_debut,
        case 
            when lower(trim(date_heure_fin)) in ('nat', 'nan') then null
            when date_heure_fin like '01/01/0001%' then null
            else TIMESTAMP(date_heure_fin)
        end as date_heure_fin,
        case 
            when lower(trim(creation_date)) in ('nat', 'nan') then null
            when creation_date like '01/01/0001%' then null
            else TIMESTAMP(creation_date)
        end as creation_date,
        case 
            when lower(trim(pickup_date)) in ('nat', 'nan') then null
            when pickup_date like '01/01/0001%' then null
            else TIMESTAMP(pickup_date)
        end as pickup_date,
        case 
            when lower(trim(date_planning_nomadrepair)) in ('nat', 'nan') then null
            when date_planning_nomadrepair like '01/01/0001%' then null
            else TIMESTAMP(date_planning_nomadrepair)
        end as date_planning_nomadrepair,

        -- Metadata
        timestamp(extracted_at) as extracted_at,
        source_file
        
    from source_data
)

select * from cleaned_data