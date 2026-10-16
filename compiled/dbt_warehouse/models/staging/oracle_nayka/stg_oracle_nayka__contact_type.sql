

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_contact_type`
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idcontact_type as int64) as idcontact_type,
        code,
        code_type,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data