{{ config(materialized='table') }}

-- Ventes Nunshen par mois et référence (Sage) pour Cockpit Supply.
-- Factures (types 6 et 7, avoirs compris, quantités déjà signées), lignes
-- valorisées seulement (dl_valorise = 1 : sans les composants de kits, qui doubleraient
-- le CA). Référence brute (alias appliqués par l'app).
select
    extract(year from dl.do_date) as annee,
    extract(month from dl.do_date) as mois,
    dl.ar_ref as reference,
    ar.ar_design as designation,
    ar.fa_code_famille as code_famille,
    -- Avoir financier (quantité négative sans retour en stock) : CA déduit, quantité non
    -- (règle de l'export « Ventes » de Sage)
    cast(sum(if(dl.dl_qte < 0 and dl.dl_mvt_stock = 0, 0, dl.dl_qte)) as float64) as qte_vendue,
    cast(sum(dl.dl_montant_ht) as float64) as ca_ht,
    max(dl.extracted_at) as extracted_at
from {{ ref('stg_mssql_sage__f_docligne') }} as dl
left join {{ ref('stg_mssql_sage__f_article') }} as ar
    on dl.ar_ref = ar.ar_ref
where
    dl.do_domaine = 0
    and dl.do_type in (6, 7)
    and dl.dl_valorise = 1
    and dl.ar_ref is not null
    and dl.do_date >= timestamp('2024-01-01')
group by annee, mois, reference, designation, code_famille
