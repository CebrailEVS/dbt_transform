{{ config(
    materialized='table',
    cluster_by=['entity_code', 'product_code']
) }}

-- Jour de photo précédent : Distrilog n'exporte pas les stocks nuls, un article absent de la photo de la
-- veille y était donc à 0.
with photos as (
    select
        snapshot_date,
        lag(snapshot_date) over (order by snapshot_date) as snapshot_date_veille
    from (
        select distinct snapshot_date
        from {{ ref('int_oracle_neshu__stock_theorique') }}
    ) as jours
),

-- Stock de la veille lu sur toutes les lignes de la série, date d'inventaire renseignée ou non ;
-- NULL le premier jour de l'historique (veille inconnue).
serie as (
    select
        st.entity_code,
        st.product_code,
        st.date_system,
        st.date_inventaire,
        st.stock_at_date,
        st.stock_inventaire,
        st.is_vehicle_active,
        case
            when ph.snapshot_date_veille is null then null
            when lag(st.snapshot_date) over par_serie = ph.snapshot_date_veille
                then lag(st.stock_at_date) over par_serie
            else 0
        end as stock_veille
    from {{ ref('int_oracle_neshu__stock_theorique') }} as st
    inner join photos as ph on st.snapshot_date = ph.snapshot_date
    window par_serie as (partition by st.entity_code, st.product_code order by st.snapshot_date)
),

lignes as (
    select
        entity_code,
        product_code,
        date_system,
        date_inventaire,
        stock_at_date,
        stock_inventaire,
        stock_veille
    from serie
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
        stock_veille,
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
    stock_veille,
    stock_inventaire,
    stock_at_date
from avec_precedent
where
    date_inventaire_precedente is null
    or date_inventaire != date_inventaire_precedente
