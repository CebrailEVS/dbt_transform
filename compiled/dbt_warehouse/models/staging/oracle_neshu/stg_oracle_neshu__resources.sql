

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_resources`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idresources as int64) as idresources,
        cast(idresources_type as int64) as idresources_type,
        cast(resources_idresources as int64) as resources_idresources,
        cast(idcompany as int64) as idcompany,
        cast(idcompany_storehouse as int64) as idcompany_storehouse,
        cast(idlocation as int64) as idlocation,
        cast(idcontact as int64) as idcontact,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        -- Colonnes texte
        code,
        name,
        code_status_record,

        -- Colonnes numériques
        cast(cost as float64) as cost,

        -- Timestamps harmonisés
        date(arrival) as arrival,
        timestamp(datetime(departure), 'Europe/Paris') as departure,

        -- Timestamps harmonisés
        timestamp(datetime(creation_date), 'Europe/Paris') as created_at,
        -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(datetime(coalesce(modification_date, creation_date)), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data