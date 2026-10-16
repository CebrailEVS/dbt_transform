

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_company_has_location`
),

cleaned_data as (
    select
        cast(idcompany as int64) as idcompany,
        cast(idlocation as int64) as idlocation,
        cast(idlocation_type as int64) as idlocation_type,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data