

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_company_has_location`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlocation as int64) as idlocation,
        cast(idcompany as int64) as idcompany,
        cast(idlocation_type as int64) as idlocation_type,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data