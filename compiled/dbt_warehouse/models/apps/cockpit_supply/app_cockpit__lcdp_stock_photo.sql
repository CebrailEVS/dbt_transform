

-- Photos de référence de chaque série (entité × article) dans chaque mois : la
-- dernière (fin de mois) et la première (repli de l'app quand la fin de mois
-- manque). Par SÉRIE et non par jour global : l'app cherche la photo d'un groupe
-- (un dépôt, un préfixe d'articles…) dans les seules lignes de ce groupe, y compris
-- une série arrêtée en cours de mois.
with series as (
    select
        *,
        snapshot_date = max(snapshot_date) over par_serie_mois as is_derniere_photo_mois,
        snapshot_date = min(snapshot_date) over par_serie_mois as is_premiere_photo_mois
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
    window par_serie_mois as (
        partition by entity_type, id_entity, product_code, date_trunc(snapshot_date, month)
    )
)

select
    snapshot_date,
    entity_type,
    id_entity,
    product_code,
    entity_code,
    entity_name,
    product_name,
    date_inventaire,
    date_system,
    is_vehicle_active,
    is_derniere_photo_mois,
    is_premiere_photo_mois,
    snapshot_date = max(snapshot_date) over () as is_photo_courante,
    stock_at_date,
    stock_inventaire,
    plus,
    moins,
    dpa,
    purchase_price
from series
where is_derniere_photo_mois or is_premiere_photo_mois