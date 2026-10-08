{{ config(materialized='table') }}

-- Nomenclatures Nunshen (Sage, niveau 1) pour Cockpit Supply : remplace l'import
-- « Nomenclature », avec en plus la quantité de chaque composant.
select
    nm.ar_ref as reference,
    p.ar_design as designation,
    p.fa_code_famille as code_famille,
    nm.no_ref_det as composant_reference,
    c.ar_design as composant_designation,
    cast(nm.no_qte as float64) as quantite_composant,
    nm.extracted_at
from {{ ref('stg_mssql_sage__f_nomenclat') }} as nm
left join {{ ref('stg_mssql_sage__f_article') }} as p
    on nm.ar_ref = p.ar_ref
left join {{ ref('stg_mssql_sage__f_article') }} as c
    on nm.no_ref_det = c.ar_ref
