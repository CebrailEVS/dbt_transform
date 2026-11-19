-- Le mart de détail contient toutes les interventions en alerte de l'intermédiaire source, plus
-- celles à pose sans stock, et aucune autre : toute ligne a une alerte de l'intermédiaire ou
-- porte POSE SANS STOCK. Retourne une ligne par écart.
with source as (
    select
        n_planning,
        alerte_compile
    from {{ ref('int_nesp_tech__alerte_conso_aguila') }}
    where coalesce(alerte_compile, '') != ''
),

detail as (
    select
        n_planning,
        alerte_compile
    from {{ ref('fct_technique__alerte_conso_aguila_detail') }}
)

select
    source.n_planning,
    'alerte intermediaire absente du mart' as ecart
from source
left join detail
    on source.n_planning = detail.n_planning
where detail.n_planning is null

union all

select
    detail.n_planning,
    'ligne sans alerte intermediaire ni POSE SANS STOCK' as ecart
from detail
left join source
    on detail.n_planning = source.n_planning
where
    source.n_planning is null
    and coalesce(detail.alerte_compile, '') not like '%POSE SANS STOCK%'
