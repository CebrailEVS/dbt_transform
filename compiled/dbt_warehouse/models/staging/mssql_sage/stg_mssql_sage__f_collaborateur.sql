

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_collaborateur`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Champs principaux
        co_no,
        co_nom,
        co_prenom,
        co_fonction,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at

    from source_data
)

select *
from cleaned_data
qualify row_number() over (
    partition by co_no
    order by updated_at desc, cb_marq desc
) = 1