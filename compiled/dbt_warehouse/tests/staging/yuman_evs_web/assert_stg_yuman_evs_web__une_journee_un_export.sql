-- Une journée vient d'un seul export : le rang `_ligne` ne se compare qu'à cette condition. Deux
-- exports pour une même journée signalent deux runs du pipeline lancés en même temps, qui l'ont
-- chargée deux fois.
select
    journee,
    count(distinct _export_yuman) as nb_exports,
    count(*) as nb_lignes
from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_web__mouvements_stock`
group by journee
having count(distinct _export_yuman) > 1