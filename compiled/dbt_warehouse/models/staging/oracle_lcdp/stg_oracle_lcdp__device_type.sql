

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_device_type`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(iddevice_type as int64) as iddevice_type,

        -- Colonnes texte
        code,
        code_type,

        -- Timestamps harmonisés
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data