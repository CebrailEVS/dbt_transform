
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu_gcs__stock_products`
      
    partition by timestamp_trunc(date_system, day)
    

    
    OPTIONS(
      description="""Table marts des suivi inventaire th\u00e9orique VEHICULE Actif + D\u00e9p\u00f4t quotidiens de stock th\u00e9orique Oracle Neshu depuis la table staging"""
    )
    as (
      

with resources_labels as (
    select
        r.idresources,
        r.idresources_type,
        r.code as resources_code,
        r.name as resources_name,
        lh.idlabel,
        l.code as label_code,
        lf.code as label_family_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_resources` as lh
        on r.idresources = lh.idresources
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lh.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
),

aggregated_labels as (
    select
        idresources,
        idresources_type,
        resources_code,
        resources_name,
        max(case when label_family_code = 'isactive' then label_code end) as is_active
    from resources_labels
    where idresources_type = 3  -- type vehicule
    group by 1, 2, 3, 4
),

filtered_stock as (
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
        st.extracted_at,
        st.file_datetime
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu_gcs__stock_theorique` as st
    left join aggregated_labels as al
        on
            st.id_entity = al.idresources
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
            and al.is_active = 'yes'
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
    extracted_at,
    file_datetime
from filtered_stock
    );
  