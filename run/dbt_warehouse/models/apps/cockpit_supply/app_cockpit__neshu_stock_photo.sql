
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_stock_photo`
      
    partition by snapshot_date
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Photos de stock Neshu dont l'application Cockpit Supply a besoin : pour chaque s\u00e9rie (entit\u00e9 \u00d7 article) et chaque mois, la derni\u00e8re photo (stock de fin de mois, ou photo courante pour le mois en cours) et la premi\u00e8re (repli de l'app quand la photo de fin de mois manque).\n[COMMENT CONSTRUITE] Sous-ensemble de fct_supply_chain__stock_neshu : lignes dont snapshot_date est le min ou le max de la s\u00e9rie (entity_type, id_entity, product_code) dans son mois. Colonnes reprises sans transformation, plus trois indicateurs de type de photo.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code), comme la table source, sur environ 2 jours par s\u00e9rie et par mois.\n[NOTES] L'app applique elle-m\u00eame ses r\u00e8gles de fin de mois (api, stock_snapshot_fin_mois) : ce mod\u00e8le ne fait que lui fournir les jours n\u00e9cessaires, d'o\u00f9 des chiffres strictement identiques. Bornes calcul\u00e9es PAR S\u00c9RIE et non par jour global : l'app cherche la photo d'un groupe dans les seules lignes de ce groupe, et une s\u00e9rie arr\u00eat\u00e9e en cours de mois (cas r\u00e9el : caf\u00e9 torr\u00e9fi\u00e9 au d\u00e9p\u00f4t VERT) doit garder sa derni\u00e8re ligne. date_system tombe toujours le jour de snapshot_date. Les v\u00e9hicules inactifs ne sont pas filtr\u00e9s (l'app applique son propre filtre). R\u00e9serv\u00e9 \u00e0 l'application : ne pas brancher de rapport Power BI dessus.\n"""
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
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
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
  