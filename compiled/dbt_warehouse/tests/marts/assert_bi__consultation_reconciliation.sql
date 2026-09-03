

-- Les TROIS faits de la BU derivent du meme filtre `activity = 'ViewReport'`,
-- reecrit dans trois fichiers distincts. Le voisinage est piegeux : il existe
-- une activite `ExportReport` separee (16 evenements), en plus des
-- consultations dont `consumption_method = 'Export Report'` (28). Une
-- correction appliquee a un seul des trois fichiers passerait tous les autres
-- tests sans que rien ne signale la divergence.
--
-- Invariant : le nombre de lignes du fait atomique doit egaler la somme des
-- consultations des deux faits agreges. Verifie a 246 le 2026-09-03.
-- En `error` : ce n'est pas une alerte metier, c'est une incoherence interne
-- qui ne doit jamais atteindre Power BI.

with atomique as (
    select count(*) as n from `evs-datastack-prod`.`prod_marts`.`fct_bi__consultation`
),

journalier as (
    select sum(nb_consultations) as n from `evs-datastack-prod`.`prod_marts`.`fct_bi__activite_rapport_jour`
),

etat_usage as (
    select sum(nb_consultations) as n from `evs-datastack-prod`.`prod_marts`.`fct_bi__usage_rapport`
)

select
    a.n as n_atomique,
    j.n as n_journalier,
    u.n as n_usage
from atomique as a
cross join journalier as j
cross join etat_usage as u
where a.n != j.n or a.n != u.n