

-- Pièces Nespresso posées, ligne à ligne, mêmes filtres que la table mensuelle :
-- jamais chargée en mémoire, interrogée à la demande par l'app (détail d'un mois pour
-- l'export du flux TechCare, conso jour par jour d'une référence sur 12 mois).
select
    date_heure_debut,
    code_article,
    coalesce(upper(trim(product_reference)), 'NONE') as product_reference,
    technician_id,
    nom_article,
    coalesce(qty_consommee, 0) as qty_consommee
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
where
    etat_intervention in ('terminée signée', 'terminée non signée')
    and date_heure_debut is not null
    and coalesce(upper(trim(code_article)), 'NONE') not in ('0000001', '0000100')