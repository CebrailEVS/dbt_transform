

-- La photo ne porte que le code article, et un même code peut désigner des fiches de types
-- différents : il reste alors sans type, plutôt que d'en recevoir un au hasard.
with produit as (
    select
        p.code as product_code,
        if(count(distinct pt.code) = 1, any_value(pt.code), null) as product_type
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product` as p
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product_type` as pt
        on p.idproduct_type = pt.idproduct_type
    where coalesce(p.code_status_record, 0) != -1
    group by p.code
)

select
    st.snapshot_date,
    st.id_entity,
    st.entity_type,
    st.product_code,
    st.resources_code as entity_code,
    st.entity_name,
    case when st.entity_type = 'company' then case
    when st.resources_code = 'DEPOTREBUT' then 'rebut'
    when st.resources_code = 'MARKETING' then 'hors_stock'
    else 'stock'
end end as depot_role,
    st.product_name,
    pr.product_type,
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
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka_gcs__stock_theorique` as st
left join produit as pr
    on st.product_code = pr.product_code