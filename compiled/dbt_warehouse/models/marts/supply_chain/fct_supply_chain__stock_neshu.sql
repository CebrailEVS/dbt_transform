

with filtered_stock as (
    select
        st.snapshot_date,
        st.id_entity,
        st.entity_type,
        st.product_code,
        st.resources_code as entity_code,
        st.entity_name,
        -- Exposée, et NON filtrante : c'est un état COURANT, non historisé. La
        -- filtrer ici ferait disparaître rétroactivement de tout l'historique un
        -- véhicule désactivé depuis, et réapparaître un véhicule réactivé.
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
            -- Seul filtre conservé sur les ressources : le TYPE, qui ne bouge pas.
            -- Il exclut la PERSON présente dans le stock.
            and r.resources_type = 'VEHICLE'
        )
)

select
    snapshot_date,
    id_entity,
    entity_type,
    product_code,
    entity_code,
    entity_name,
    is_vehicle_active,
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