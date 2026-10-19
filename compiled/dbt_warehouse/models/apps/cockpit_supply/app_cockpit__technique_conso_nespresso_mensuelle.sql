

-- Pièces Nespresso posées par les techniciens, par mois, référence et technicien : grain de tous les calculs
-- mensuels de l'app (conso par dépôt, par technicien, index saisonnier, facturation). Agrégat du détail, donc
-- mêmes filtres (interventions réalisées, date renseignée, codes de main-d'œuvre exclus). Le mois est celui de
-- date_heure_debut en UTC, comme l'app.
select
    format_timestamp('%Y-%m', date_heure_debut) as mois,
    product_reference,
    technician_id,
    sum(qty_consommee) as qte,
    max(nom_article) as nom_article
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_detail`
group by mois, product_reference, technician_id