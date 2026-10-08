
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_stock_photo`
      
    partition by snapshot_date
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Photos de stock Caf\u00e9s du Phare dont l'application Cockpit Supply a besoin : pour chaque s\u00e9rie (entit\u00e9 \u00d7 article) et chaque mois, la derni\u00e8re et la premi\u00e8re photo.\n[COMMENT CONSTRUITE] Sous-ensemble de fct_supply_chain__stock_lcdp : lignes dont snapshot_date est le min ou le max de la s\u00e9rie (entity_type, id_entity, product_code) dans son mois (m\u00eame construction que app_cockpit__neshu_stock_photo). Colonnes reprises sans transformation, plus trois indicateurs de type de photo.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code), sur environ 2 jours par mois.\n[NOTES] Remplace dans l'app la lecture de tout l'historique journalier (~250 k lignes \u2192 ~19 k). R\u00e8gles de fin de mois, prix effectif et kg appliqu\u00e9s par l'app (api/lcdp.py). date_system tombe toujours le jour de snapshot_date (v\u00e9rifi\u00e9 le 2026-10-08). R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Photos de référence de chaque série (entité × article) dans chaque mois : la
-- dernière (fin de mois) et la première (repli de l'app quand la fin de mois
-- manque). Par SÉRIE et non par jour global : l'app cherche la photo d'un groupe
-- (un dépôt, un préfixe d'articles…) dans les seules lignes de ce groupe, y compris
-- une série arrêtée en cours de mois.
with series as (
    select
        *,
        snapshot_date = max(snapshot_date) over par_serie_mois as is_derniere_photo_mois,
        snapshot_date = min(snapshot_date) over par_serie_mois as is_premiere_photo_mois
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
    window par_serie_mois as (
        partition by entity_type, id_entity, product_code, date_trunc(snapshot_date, month)
    )
)

select
    snapshot_date,
    entity_type,
    id_entity,
    product_code,
    entity_code,
    entity_name,
    product_name,
    date_inventaire,
    date_system,
    is_vehicle_active,
    is_derniere_photo_mois,
    is_premiere_photo_mois,
    snapshot_date = max(snapshot_date) over () as is_photo_courante,
    stock_at_date,
    stock_inventaire,
    plus,
    moins,
    dpa,
    purchase_price
from series
where is_derniere_photo_mois or is_premiere_photo_mois
    );
  