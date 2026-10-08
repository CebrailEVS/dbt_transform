

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