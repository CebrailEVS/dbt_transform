

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_historic_action`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idhistoric_action as int64) as idhistoric_action,
        cast(idstring as int64) as idstring,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data