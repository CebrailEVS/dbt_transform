

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_lotserie`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Lot et article
        ar_ref,
        ls_no_serie,
        de_no,

        -- Mouvements (lignes de document)
        dl_no_in,
        dl_no_out,
        ls_mvt_stock,

        -- Quantités
        ls_qte,
        ls_qte_restant,
        ls_qte_res,
        ls_lot_epuise,

        -- Dates
        nullif(ls_fabrication, timestamp('1753-01-01')) as ls_fabrication,
        nullif(ls_peremption, timestamp('1753-01-01')) as ls_peremption,

        -- Libre
        nullif(trim(ls_complement), '') as ls_complement,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data