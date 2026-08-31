
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
      
    partition by snapshot_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock th\u00e9orique journalier des produits LCDP, par v\u00e9hicule et par d\u00e9p\u00f4t.\n[COMMENT CONSTRUITE] Issu de stg_oracle_lcdp_gcs__stock_theorique (pipeline dlt oracle_lcdp_stock, batch Oracle de 23:15 Paris). Aucun filtre : les 11 d\u00e9p\u00f4ts (entity_type='company') et tous les v\u00e9hicules (entity_type='resource') sont expos\u00e9s. is_vehicle_active aplati depuis is_active de dim_lcdp__resource pour les v\u00e9hicules (FALSE si absent de la dim), NULL pour les d\u00e9p\u00f4ts. is_out_of_stock d\u00e9riv\u00e9 de stock_at_date=0. dpa fallback sur purchase_price si manquant.\n[GRAIN] 1 ligne par (snapshot_date, entity_type, id_entity, product_code).\n[NOTES] D\u00e9marr\u00e9 le 2026-06-05 (pas d'historique ant\u00e9rieur). Filtrer sur snapshot_date : c'est une DATE, insensible \u00e0 l'heure du run comme au fuseau de lecture. date_system est conserv\u00e9e \u00e0 c\u00f4t\u00e9 pour l'audit \u2014 elle porte l'heure du batch, donc r\u00e9v\u00e8le une journ\u00e9e rejou\u00e9e \u2014 mais n'est pas une cl\u00e9 de filtre. entity_type fait partie du grain, par prudence : c\u00f4t\u00e9 Neshu idcompany et idresources collisionnent, LCDP n'y \u00e9chappe aujourd'hui que par chance. plus/moins = \u00e9carts d'inventaire vs th\u00e9orique. Stocks n\u00e9gatifs possibles (stock th\u00e9orique). date_inventaire NULL \u2248 30 % (entit\u00e9s/produits jamais inventori\u00e9s).\n"""
    )
    as (
      

select
    st.snapshot_date,
    st.id_entity,
    st.entity_type,
    st.product_code,
    st.resources_code as entity_code,
    st.entity_name,
    case
        when st.entity_type = 'resource' then coalesce(r.is_active, false)
    end as is_vehicle_active,
    st.product_name,
    st.stock_at_date,
    st.stock_at_date = 0 as is_out_of_stock,
    st.date_inventaire,
    st.stock_inventaire,
    st.plus,
    st.moins,
    coalesce(st.dpa, st.purchase_price) as dpa,
    st.purchase_price,
    st.date_system,
    st.extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp_gcs__stock_theorique` as st
left join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource` as r
    on
        st.id_entity = r.resources_id
        and st.entity_type = 'resource'
    );
  