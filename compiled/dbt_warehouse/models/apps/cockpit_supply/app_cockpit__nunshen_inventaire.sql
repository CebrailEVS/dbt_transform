

-- Sessions d'inventaire Nunshen (Sage, pièces « i… » d'entrée et de sortie, types 20/21)
-- pour Cockpit Supply (fréquence des inventaires). Une session = une date × un dépôt. Sage n'écrit une pièce
-- d'inventaire que s'il y a un écart : un inventaire sans écart n'y figure pas.
select
    date(dl.do_date) as date_inventaire,
    coalesce(nullif(dl.de_no, 0), de.de_no) as depot_id,
    dep.de_intitule as depot,
    count(distinct dl.do_piece) as nb_pieces,
    count(distinct dl.ar_ref) as nb_references,
    cast(sum(if(dl.do_type = 20, dl.dl_qte, 0)) as float64) as qte_entree,
    cast(sum(if(dl.do_type = 21, dl.dl_qte, 0)) as float64) as qte_sortie,
    cast(sum(if(dl.do_type = 20, dl.dl_montant_ht, 0)) as float64) as valeur_entree,
    cast(sum(if(dl.do_type = 21, dl.dl_montant_ht, 0)) as float64) as valeur_sortie,
    max(dl.extracted_at) as extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docentete` as de
    on dl.do_type = de.do_type and dl.do_piece = de.do_piece
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot` as dep
    on coalesce(nullif(dl.de_no, 0), de.de_no) = dep.de_no
where
    dl.do_domaine = 2
    and dl.do_type in (20, 21)
    and starts_with(lower(dl.do_piece), 'i')
group by date_inventaire, depot_id, depot