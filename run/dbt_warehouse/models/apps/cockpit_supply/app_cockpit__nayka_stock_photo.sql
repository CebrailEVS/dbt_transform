
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nayka_stock_photo`
      
    partition by snapshot_date
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Photos de stock Nayka dont l'application Cockpit Supply a besoin pour la cl\u00f4ture mensuelle : pour chaque s\u00e9rie (d\u00e9p\u00f4t \u00d7 article) et chaque mois, la derni\u00e8re et la premi\u00e8re photo.\n[COMMENT CONSTRUITE] Sous-ensemble de int_oracle_nayka__stock_theorique : lignes dont snapshot_date est le min ou le max de la s\u00e9rie (entity_type, id_entity, product_code) dans son mois (m\u00eame construction que app_cockpit__lcdp_stock_photo). Colonnes reprises sans transformation, plus trois indicateurs de type de photo.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code), sur environ 2 jours par mois.\n[NOTES] Lu par la cl\u00f4ture mensuelle Nayka. Les mod\u00e8les de machines (product_type = MODELE) figurent ici en quantit\u00e9 alors que chaque machine est aussi une ligne de app_cockpit__nayka_equipement_stock : ne jamais additionner les deux. P\u00e9rim\u00e8tre valoris\u00e9 et r\u00e8gles de valorisation appliqu\u00e9s par l'app. date_system tombe toujours le jour de snapshot_date. R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Photos de référence de chaque série (entité × article) dans chaque mois : la
-- dernière (fin de mois) et la première (repli de l'app quand la fin de mois
-- manque). Par SÉRIE et non par jour global : l'app cherche la photo d'un groupe
-- (un dépôt, un type d'article…) dans les seules lignes de ce groupe, y compris
-- une série arrêtée en cours de mois.
with series as (
    select
        *,
        snapshot_date = max(snapshot_date) over par_serie_mois as is_derniere_photo_mois,
        snapshot_date = min(snapshot_date) over par_serie_mois as is_premiere_photo_mois
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_nayka__stock_theorique`
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
    depot_role,
    product_name,
    product_type,
    date_inventaire,
    date_system,
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
  