

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_currency`
),

cleaned_data as (
    select
        cast(idcurrency as int64) as idcurrency,
        code,
        symbole,
        cast(isactif as int64) as isactif,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data