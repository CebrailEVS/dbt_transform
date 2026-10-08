

-- Point de commande Neshu (« Commandes à passer »), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    product_id,
    company_code,
    product_code,
    product_name,
    product_family,
    statut_vie,
    classe_abc,
    methode_prevision,
    unite_commande_code,
    delai_jours,
    z_service,
    demande_prevue_mensuelle,
    demande_prevue_journaliere,
    sigma_demande_mensuelle,
    stock_actuel,
    encours_fournisseur,
    position_stock,
    stock_securite,
    point_commande,
    semaines_surstock_max,
    stock_max,
    statut_reappro,
    quantite_excedentaire,
    coeff_conditionnement,
    quantite_a_commander,
    quantite_a_commander_conditionnee,
    date_calcul,
    is_article_arrete_avec_stock
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`