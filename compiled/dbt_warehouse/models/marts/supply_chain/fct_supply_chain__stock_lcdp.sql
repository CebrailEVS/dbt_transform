

select
    st.id_entity,
    st.entity_type,
    st.resources_code as entity_code,
    st.entity_name,
    case
        when st.entity_type = 'resource' then coalesce(r.is_active, false)
    end as is_active,
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
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp_gcs__stock_theorique` as st
left join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource` as r
    on
        st.id_entity = r.resources_id
        and st.entity_type = 'resource'