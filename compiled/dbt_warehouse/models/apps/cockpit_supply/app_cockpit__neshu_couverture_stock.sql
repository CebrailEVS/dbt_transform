

-- Couverture de stock des dépôts Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    date_calcul,
    company_id,
    entity_code,
    entity_name,
    product_code,
    product_name,
    stock_actuel,
    conso_journaliere_n1,
    conso_mensuelle_moy_n1,
    jours_couverture,
    qte_a_commander
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__couverture_stock`