

with lignes as (
    select
        entity_code,
        product_code,
        date_system,
        date_inventaire,
        stock_at_date,
        stock_inventaire
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
    where
        date_inventaire is not null
        and date_system is not null
        -- Même périmètre que l'app (_filter_vehicule_actif) : on écarte les véhicules
        -- marqués inactifs ; un état inconnu (NULL) est conservé.
        and not (
            (
                regexp_contains(entity_code, r'^V\d+')
                or entity_code in ('ARKEMA', 'STRASBOURG', 'RATP')
            )
            and coalesce(is_vehicle_active = false, false)
        )
),

avec_precedent as (
    select
        entity_code,
        product_code,
        date_system,
        date_inventaire,
        stock_at_date,
        stock_inventaire,
        lag(date_inventaire) over par_article as date_inventaire_precedente,
        lag(stock_at_date) over par_article as stock_at_date_precedent
    from lignes
    window par_article as (partition by entity_code, product_code order by date_system)
)

select
    date_system,
    entity_code,
    product_code,
    date_inventaire,
    date_inventaire_precedente,
    date_inventaire_precedente is null as is_premiere_ligne,
    stock_at_date_precedent,
    stock_inventaire,
    stock_at_date
from avec_precedent
where
    date_inventaire_precedente is null
    or date_inventaire != date_inventaire_precedente