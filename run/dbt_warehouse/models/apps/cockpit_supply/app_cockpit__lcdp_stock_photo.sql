
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_stock_photo`
      
    partition by snapshot_date
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Photos de stock Caf\u00e9s du Phare dont l'application Cockpit Supply a besoin : derni\u00e8re et premi\u00e8re photo de chaque mois, donc la photo courante.\n[COMMENT CONSTRUITE] Sous-ensemble de fct_supply_chain__stock_lcdp restreint aux jours de r\u00e9f\u00e9rence (min et max de snapshot_date par mois). Colonnes reprises sans transformation, plus trois indicateurs de type de photo.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code), sur environ 2 jours par mois.\n[NOTES] Remplace dans l'app la lecture de tout l'historique journalier (~250 k lignes \u2192 ~19 k). R\u00e8gles de fin de mois, prix effectif et kg appliqu\u00e9s par l'app (api/lcdp.py). date_system tombe toujours le jour de snapshot_date (v\u00e9rifi\u00e9 le 2026-10-08). R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Même construction que app_cockpit__neshu_stock_photo (photos de référence du mois).
with jours as (
    select distinct snapshot_date
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
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
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp` as st
inner join bornes as b
    on st.snapshot_date = b.snapshot_date
    );
  