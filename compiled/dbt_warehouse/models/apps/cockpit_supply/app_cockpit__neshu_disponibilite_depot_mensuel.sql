

-- Disponibilité mensuelle des articles dans les dépôts Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    mois,
    company_id,
    entity_code,
    product_code,
    taux_disponibilite_pct
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_depot_mensuel`