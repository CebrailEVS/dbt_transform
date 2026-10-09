

-- Nomenclatures Nunshen (Sage, niveau 1) pour Cockpit Supply, avec la quantité de chaque composant.
select
    nm.ar_ref as reference,
    p.ar_design as designation,
    p.fa_code_famille as code_famille,
    nm.no_ref_det as composant_reference,
    c.ar_design as composant_designation,
    cast(nm.no_qte as float64) as quantite_composant,
    nm.extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_nomenclat` as nm
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as p
    on nm.ar_ref = p.ar_ref
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as c
    on nm.no_ref_det = c.ar_ref