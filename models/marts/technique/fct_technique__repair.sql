{{ config(
    materialized='table',
    partition_by={'field': 'intervention_date', 'data_type': 'date'},
    cluster_by=['material_id', 'client_id', 'technician_id']
) }}

-- Qualification "repair" des interventions curatives NESHU : une curative
-- réalisée sur une machine ayant déjà eu une curative dans les 30 jours
-- précédents est un repair (récidive de panne / échec de réparation).
-- Les curatives espacées de <= 30 jours sont chaînées en épisodes pour
-- compter les multi-visites sans chevauchement.

with curative_interventions as (
    select
        workorder_id,
        demand_id,
        material_id,
        site_id,
        client_id,
        technician_id,
        client_name,
        site_name,
        material_serial_number,
        machine_clean,
        famille_neshu,
        workorder_technician_name,
        demand_description,
        workorder_report,
        date_done
    from {{ ref('int_yuman__interventions') }}
    where
        partner_name = 'NESHU'
        and intervention_state = 'REALISEE'
        and workorder_type_clean = 'curative'
        -- Sans machine identifiée, aucun suivi repair possible (38 lignes exclues)
        and material_id is not null
),

-- Articles consommés par intervention, agrégés en liste lisible BI
workorder_articles as (
    select
        workorder_id,
        string_agg(distinct product_reference, ', ' order by product_reference) as articles
    from {{ ref('stg_yuman__workorder_products') }}
    group by workorder_id
),

with_articles as (
    select
        ci.*,
        wa.articles
    from curative_interventions as ci
    left join workorder_articles as wa
        on ci.workorder_id = wa.workorder_id
),

-- Contexte de la curative précédente sur la même machine
with_previous as (
    select
        *,
        lag(workorder_id) over (
            partition by material_id order by date_done, workorder_id
        ) as previous_workorder_id,
        lag(date_done) over (
            partition by material_id order by date_done, workorder_id
        ) as previous_done_at,
        lag(workorder_technician_name) over (
            partition by material_id order by date_done, workorder_id
        ) as previous_technician_name,
        lag(demand_description) over (
            partition by material_id order by date_done, workorder_id
        ) as previous_demand_description,
        lag(workorder_report) over (
            partition by material_id order by date_done, workorder_id
        ) as previous_report,
        lag(articles) over (
            partition by material_id order by date_done, workorder_id
        ) as previous_articles
    from with_articles
),

-- Chaînage en épisodes : nouvel épisode si première curative de la machine
-- ou si la précédente date de plus de 30 jours
episodes as (
    select
        *,
        date_diff(date(date_done), date(previous_done_at), day) as delai_jours_depuis_precedente,
        countif(
            previous_done_at is null
            or date_diff(date(date_done), date(previous_done_at), day) > 30
        ) over (
            partition by material_id
            order by date_done, workorder_id
            rows between unbounded preceding and current row
        ) as episode_seq
    from with_previous
),

final as (
    select
        *,
        coalesce(delai_jours_depuis_precedente <= 30, false) as is_repair,
        {{ dbt_utils.generate_surrogate_key(['material_id', 'episode_seq']) }} as episode_id,
        row_number() over (
            partition by material_id, episode_seq order by date_done, workorder_id
        ) as episode_rank,
        count(*) over (partition by material_id, episode_seq) as nb_visites_episode
    from episodes
)

select
    -- Grain
    date(date_done) as intervention_date,
    workorder_id,

    -- FK
    material_id,
    client_id,
    site_id,
    technician_id,
    demand_id,
    episode_id,

    -- Attributs intervention
    client_name,
    site_name,
    material_serial_number,
    machine_clean,
    famille_neshu,
    workorder_technician_name,
    demand_description,
    workorder_report,
    articles,

    -- Contexte de la curative précédente
    previous_workorder_id,
    date(previous_done_at) as previous_intervention_date,
    previous_technician_name,
    previous_demand_description,
    previous_report,
    previous_articles,

    -- Flags
    is_repair,

    -- Mesures / rangs
    delai_jours_depuis_precedente,
    episode_rank,
    nb_visites_episode,

    -- Métadonnées
    date_done as done_at
from final
