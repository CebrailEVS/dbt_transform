

-- Statut COURANT des ressources, non historisé : exposé sans filtrer.
with ressource as (
    select
        r.idresources,
        coalesce(lower(max(case when lf.code = 'ISACTIVE' then l.code end)) = 'yes', false) as is_active
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources` as r
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources_type` as rt
        on r.idresources_type = rt.idresources_type
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_resources` as lhr
        on r.idresources = lhr.idresources
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as l
        on lhr.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    where rt.code in ('PERSON', 'VEHICLE')
    group by r.idresources
)

select
    st.snapshot_date,
    st.id_entity,
    st.entity_type,
    st.product_code,
    st.resources_code as entity_code,
    st.entity_name,
    case when st.entity_type = 'resource' then coalesce(r.is_active, false) end as is_vehicle_active,
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
left join ressource as r
    on st.id_entity = r.idresources and st.entity_type = 'resource'