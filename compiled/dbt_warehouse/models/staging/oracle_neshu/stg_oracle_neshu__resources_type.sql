

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_resources_type`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idresources_type as int64) as idresources_type,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data