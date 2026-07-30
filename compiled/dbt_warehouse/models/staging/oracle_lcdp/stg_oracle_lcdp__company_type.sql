

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_company_type`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcompany_type as int64) as idcompany_type,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data