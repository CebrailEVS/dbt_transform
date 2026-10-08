

-- Classification de la demande des articles Neshu (ADI / CV², saisonnalité), pour Cockpit Supply : seules les colonnes lues par l'app.
select
    company_id,
    product_id,
    classe_demande,
    alerte_exploit,
    alerte_saison,
    saison_produit,
    en_saison,
    adi,
    cv2,
    valeur_12m
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`