

-- Produits fabriqués Nunshen (Sage, bons de fabrication type 26) pour Cockpit Supply :
-- produits seulement (dl_mvt_stock = 1 ; 3 =
-- composants consommés), hors lignes MANUFACT ; quantité SOMMÉE par bon et référence (un
-- produit peut être découpé en une ligne par lot).
select
    dl.do_piece as n_piece,
    date(dl.do_date) as date_document,
    dl.ar_ref as reference,
    ar.fa_code_famille as code_famille,
    cast(sum(dl.dl_qte) as float64) as quantite,
    count(*) as nb_lignes_lot,
    max(dl.extracted_at) as extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as ar
    on dl.ar_ref = ar.ar_ref
where
    dl.do_type = 26
    and dl.dl_mvt_stock = 1
    and dl.ar_ref is not null
    and not ends_with(dl.ar_ref, 'MANUFACT')
    and dl.do_date >= timestamp('2024-01-01')
group by n_piece, date_document, reference, code_famille