

-- Statut COURANT des véhicules, non historisé : il est exposé sans filtrer, pour ne pas faire disparaître
-- rétroactivement de tout l'historique un véhicule désactivé depuis.
with vehicule as (
    select
        r.idresources,
        coalesce(lower(max(case when lf.code = 'ISACTIVE' then l.code end)) = 'yes', false) as is_active
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources_type` as rt
        on r.idresources_type = rt.idresources_type
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_resources` as lhr
        on r.idresources = lhr.idresources
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhr.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    where
        rt.code = 'VEHICLE'
        and r.code_status_record <> -1  -- ghost-deletes ERP
    group by r.idresources
)

select
    st.snapshot_date,
    st.id_entity,
    st.entity_type,
    st.product_code,
    st.resources_code as entity_code,
    st.entity_name,
    case when st.entity_type = 'resource' then v.is_active end as is_vehicle_active,
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
left join vehicule as v
    on st.id_entity = v.idresources and st.entity_type = 'resource'
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
    -- Seul filtre sur les ressources : le type VEHICLE, qui ne bouge pas ; il écarte la PERSON présente dans le stock.
    or (st.entity_type = 'resource' and v.idresources is not null)