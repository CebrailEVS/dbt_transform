

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_damping_type`
),

cleaned_data as (
    -- Retirées : idstring (table string non extraite), code_type (jamais renseigné).
    -- Pas de date de modification à la source.
    select
        cast(iddamping_type as int64) as iddamping_type,
        code,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data