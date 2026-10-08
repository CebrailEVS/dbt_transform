
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_stock_photo`
      
    partition by snapshot_date
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Photos de stock Neshu dont l'application Cockpit Supply a besoin : la derni\u00e8re photo de chaque mois (stock de fin de mois), la premi\u00e8re photo de chaque mois (repli de l'app quand la photo de fin de mois manque) et donc la photo courante (derni\u00e8re du mois en cours).\n[COMMENT CONSTRUITE] Sous-ensemble de fct_supply_chain__stock_neshu restreint aux jours de r\u00e9f\u00e9rence (min et max de snapshot_date par mois). Colonnes reprises sans transformation, plus trois indicateurs de type de photo.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code), comme la table source, sur environ 2 jours par mois.\n[NOTES] Remplace dans l'app la lecture de tout l'historique journalier (~1 M lignes \u2192 ~70 k). L'app applique elle-m\u00eame ses r\u00e8gles de fin de mois (api, stock_snapshot_fin_mois) : ce mod\u00e8le ne fait que lui fournir les jours n\u00e9cessaires, d'o\u00f9 des chiffres strictement identiques. date_system tombe toujours le jour de snapshot_date (v\u00e9rifi\u00e9 le 2026-10-08 sur 984 416 lignes). Les v\u00e9hicules inactifs ne sont pas filtr\u00e9s (l'app applique son propre filtre). R\u00e9serv\u00e9 \u00e0 l'application : ne pas brancher de rapport Power BI dessus.\n"""
    )
    as (
      

-- Photos de référence du mois : la dernière (fin de mois) et la première (repli
-- de l'app quand la fin de mois manque) ; la dernière du mois en cours est la
-- photo courante.
with jours as (
    select distinct snapshot_date
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
),

jours_reference as (
    select max(snapshot_date) as snapshot_date
    from jours
    group by date_trunc(snapshot_date, month)
    union distinct
    select min(snapshot_date) as snapshot_date
    from jours
    group by date_trunc(snapshot_date, month)
),

bornes as (
    select
        snapshot_date,
        snapshot_date = max(snapshot_date) over (partition by date_trunc(snapshot_date, month))
            as is_derniere_photo_mois,
        snapshot_date = min(snapshot_date) over (partition by date_trunc(snapshot_date, month))
            as is_premiere_photo_mois,
        snapshot_date = max(snapshot_date) over () as is_photo_courante
    from jours_reference
)

select
    st.snapshot_date,
    st.entity_type,
    st.id_entity,
    st.product_code,
    st.entity_code,
    st.entity_name,
    st.product_name,
    st.date_inventaire,
    st.date_system,
    st.is_vehicle_active,
    b.is_derniere_photo_mois,
    b.is_premiere_photo_mois,
    b.is_photo_courante,
    st.stock_at_date,
    st.stock_inventaire,
    st.plus,
    st.moins,
    st.dpa,
    st.purchase_price
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu` as st
inner join bornes as b
    on st.snapshot_date = b.snapshot_date
    );
  