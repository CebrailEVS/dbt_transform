
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
      
    partition by timestamp_trunc(date_system, day)
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock th\u00e9orique journalier des produits Neshu, par v\u00e9hicule roadman et par d\u00e9p\u00f4t.\n[COMMENT CONSTRUITE] Issu de stg_oracle_neshu_gcs__stock_theorique (export GCS quotidien Oracle Neshu), filtr\u00e9 sur les d\u00e9p\u00f4ts 01\u201305, 10, 13 (entity_type='company') et les v\u00e9hicules actifs (resources_type='VEHICLE' et is_active via dim_neshu__resource). is_out_of_stock d\u00e9riv\u00e9 de stock_at_date=0. dpa fallback sur purchase_price si manquant.\n[GRAIN] 1 ligne par (id_entity, product_code, date_system).\n[NOTES] Source GCS = export Oracle vers Cloud Storage. plus/moins = \u00e9carts d'inventaire vs th\u00e9orique.\n"""
    )
    as (
      

with filtered_stock as (
    select
        st.id_entity,
        st.entity_type,
        st.resources_code as entity_code,
        st.entity_name,
        st.product_code,
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
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu_gcs__stock_theorique` as st
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__resource` as r
        on
            st.id_entity = r.resources_id
            and st.entity_type = 'resource'
    where
        (
            st.entity_type = 'company'
            and st.entity_name in (
                '01 - rungis depot produits',
                '02 - lyon depot produits',
                '03 - bordeaux depot produits',
                '04 - strasbourg depot produits',
                '05 - perimes depot',
                '10 - rebus depot',
                '13 - marseille depot produits'
            )
        )
        or
        (
            st.entity_type = 'resource'
            and r.resources_type = 'VEHICLE'  -- exclut la PERSON présente dans le stock
            and r.is_active
        )
)

select
    id_entity,
    entity_type,
    entity_code,
    entity_name,
    product_code,
    product_name,
    stock_at_date,
    is_out_of_stock,
    date_inventaire,
    stock_inventaire,
    plus,
    moins,
    dpa,
    purchase_price,
    date_system,
    extracted_at
from filtered_stock
    );
  