-- Le mart de détail contient exactement les interventions en alerte de l'intermédiaire source :
-- ni perdue, ni ajoutée. Retourne une ligne en cas d'écart.
with source as (
    select count(*) as nb_source
    from {{ ref('int_nesp_tech__alerte_conso_aguila') }}
    where coalesce(alerte_compile, '') != ''
),

detail as (
    select count(*) as nb_detail
    from {{ ref('fct_technique__alerte_conso_aguila_detail') }}
)

select
    source.nb_source,
    detail.nb_detail
from source
cross join detail
where source.nb_source != detail.nb_detail
