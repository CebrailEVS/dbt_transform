{{ config(materialized='table') }}

-- Préparations de fabrication Nunshen ouvertes (Sage, type 24) pour Cockpit Supply :
-- remplace l'import « Production Wissous ». Chargement Sage en remplacement : une
-- préparation transformée en bon de fabrication disparaît, il ne reste que les ouvertes.
select
    dl.do_piece as n_piece,
    date(dl.do_date) as date_document,
    dl.ar_ref as reference,
    ar.ar_design as designation,
    cast(sum(dl.dl_qte) as float64) as quantite,
    max(dl.extracted_at) as extracted_at
from {{ ref('stg_mssql_sage__f_docligne') }} as dl
left join {{ ref('stg_mssql_sage__f_article') }} as ar
    on dl.ar_ref = ar.ar_ref
where dl.do_type = 24 and dl.ar_ref is not null
group by n_piece, date_document, reference, designation
