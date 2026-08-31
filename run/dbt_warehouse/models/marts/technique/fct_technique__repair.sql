
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__repair`
      
    partition by intervention_date
    cluster by material_id, client_id, technician_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nSuivi des \"repairs\" NESHU : interventions curatives r\u00e9alis\u00e9es sur une\nmachine ayant d\u00e9j\u00e0 eu une curative dans les 30 jours pr\u00e9c\u00e9dents\n(r\u00e9cidive de panne / \u00e9chec de r\u00e9paration). Contient TOUTES les curatives\nr\u00e9alis\u00e9es (flag `is_repair`), pas seulement les repairs, pour permettre\nle calcul d'un taux de repair en BI. Remplace l'export Excel mensuel\nproduit par un notebook Python.\n\n[COMMENT CONSTRUITE]\nLecture de `int_yuman__interventions` filtr\u00e9 sur `partner_name = 'NESHU'`,\n`intervention_state = 'REALISEE'`, `workorder_type_clean = 'curative'` et\n`material_id` non NULL. Articles consomm\u00e9s agr\u00e9g\u00e9s (STRING_AGG) depuis\n`stg_yuman__workorder_products`. Contexte de la curative pr\u00e9c\u00e9dente via\nLAG(partition machine, ordre date_done). Cha\u00eenage en \u00e9pisodes : nouvel\n\u00e9pisode si premi\u00e8re curative de la machine ou gap > 30 jours ; une cha\u00eene\nA\u2192B\u2192C rapproch\u00e9es = 1 \u00e9pisode de 3 visites (pas de double comptage,\ncontrairement \u00e0 une fen\u00eatre glissante).\n\n[GRAIN]\n1 ligne par `workorder_id` (intervention curative r\u00e9alis\u00e9e NESHU avec\nmachine identifi\u00e9e). ~5 600 lignes depuis d\u00e9c. 2023.\n\n[NOTES]\n- Les curatives sans `material_id` (~38 lignes) sont exclues : aucun\n  suivi repair possible sans machine.\n- Les curatives cl\u00f4tur\u00e9es avec pause historique sont INCLUSES\n  (contrairement \u00e0 l'ancien script Python qui les excluait \u00e0 tort).\n- Rafra\u00eechissement : via `source:yuman+` (pipeline EL yuman, 01:00 en\n  semaine), comme les autres marts technique.\n"""
    )
    as (
      

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
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions`
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
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_products`
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
        to_hex(md5(cast(coalesce(cast(material_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(episode_seq as string), '_dbt_utils_surrogate_key_null_') as string))) as episode_id,
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
    -- Deux curatives clôturées le même jour sur la même machine : mélange de
    -- doublons administratifs et de vrais retours — à arbitrer à l'œil en BI
    coalesce(delai_jours_depuis_precedente = 0, false) as is_same_day,

    -- Mesures / rangs
    delai_jours_depuis_precedente,
    episode_rank,
    nb_visites_episode,

    -- Métadonnées
    date_done as done_at
from final
    );
  