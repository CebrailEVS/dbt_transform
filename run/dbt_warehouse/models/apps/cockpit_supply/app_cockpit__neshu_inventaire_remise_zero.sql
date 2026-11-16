
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_inventaire_remise_zero`
      
    
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Articles remis \u00e0 0 par un comptage d'inventaire Distrilog, dans un d\u00e9p\u00f4t ou un v\u00e9hicule Neshu. Distrilog n'exporte pas les stocks nuls : l'article dispara\u00eet de la photo au lieu d'y porter sa nouvelle date d'inventaire, et app_cockpit__neshu_inventaire_transition ne le voit pas. Destin\u00e9 au contr\u00f4le \u00ab Ajustements d'inventaire \u00bb de la cl\u00f4ture Neshu de Cockpit Supply.\n[COMMENT CONSTRUITE] Disparitions de app_cockpit__neshu_stock_disparition prouv\u00e9es par une t\u00e2che \u00c9CART INVENTAIRE (is_remise_inventaire) ; le rattachement de la t\u00e2che, validation tardive comprise, est d\u00e9crit sur ce mod\u00e8le.\n[GRAIN] 1 ligne par (entity_code, product_code, snapshot_date) : un article remis \u00e0 0 entre la photo pr\u00e9c\u00e9dente et la photo snapshot_date.\n[NOTES] qte_ecart = ajustement enregistr\u00e9 par Distrilog au moment du comptage ; stock_veille = stock de la photo d'avant la disparition, qui inclut les mouvements de la journ\u00e9e. R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Distrilog n'exporte pas les stocks nuls : un article remis à 0 par un comptage disparaît de la photo au lieu d'y porter
-- sa nouvelle date d'inventaire, et app_cockpit__neshu_inventaire_transition ne le voit pas. Ce sont les disparitions
-- prouvées par une tâche ÉCART INVENTAIRE, rattachées dans app_cockpit__neshu_stock_disparition.
select
    date_system,
    snapshot_date,
    entity_code,
    product_code,
    date_inventaire,
    date_inventaire_precedente,
    stock_veille,
    qte_ecart,
    dpa_veille,
    task_id,
    task_product_id,
    task_status_code,
    nb_lignes_ecart
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_stock_disparition`
where is_remise_inventaire
    );
  