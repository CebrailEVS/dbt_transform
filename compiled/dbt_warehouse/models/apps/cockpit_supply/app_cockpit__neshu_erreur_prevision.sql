

-- Erreur de prévision mensuelle des articles Neshu, pour Cockpit Supply : seules les colonnes lues par l'app.
select
    mois_cible,
    company_id,
    product_id,
    company_code,
    product_code,
    product_name,
    classe_abc,
    methode_prevision,
    nb_mois_anciennete,
    demande_reelle,
    prevision_moyenne_mobile,
    prevision_saisonniere,
    prevision_naive,
    erreur,
    erreur_absolue
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`