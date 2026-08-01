

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_label_has_company`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idcompany as int64) as idcompany,

        -- Timestamps harmonisés
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data