{{ config(
    materialized='table',
    description='Suivi des machines en appro NESHU avec leurs interventions curatives récentes, en cours ou planifiées'
) }}

-- ============================================================
-- CTE 1 : Préparation des passages appro NESHU
-- Objectif :
--   - Récupérer les passages appro réalisés ou prévus
--   - Séparer les libellés client et machine en nom / code
-- ============================================================
with passages_appro as (

    select
        p.task_id,
        p.company_info,
        TRIM(REGEXP_EXTRACT(p.company_info, r'^(.*) - [^-]+$')) as company_name,
        TRIM(REGEXP_EXTRACT(p.company_info, r' - ([^-]+)$')) as company_code,
        p.device_info,
        TRIM(REGEXP_EXTRACT(p.device_info, r'^(.*) - [^-]+$')) as device_name,
        TRIM(REGEXP_EXTRACT(p.device_info, r' - ([^-]+)$')) as device_code,
        p.roadman_code,
        p.gea_code,
        p.task_status_code,
        p.task_start_date,
        p.task_end_date,
        p.is_done,
        p.is_planned,
        p.passage_duration_min

    from {{ ref('fct_oracle_neshu__appro') }} as p

    where
        p.task_start_date >= TIMESTAMP('2025-01-01')
        and p.task_status_code in ('FAIT', 'PREVU')
        and p.device_info is not null

),

-- ============================================================
-- CTE 2 : Classement du contexte machine
-- Objectif :
--   - Classer les passages appro du plus récent au plus ancien
--   - Préparer la conservation d’un seul contexte par machine
-- ============================================================
machine_context_ranked as (

    select
        device_code,
        device_name,
        device_info as device_label,
        company_code,
        company_name,
        company_info as company_label,
        roadman_code,
        gea_code,
        task_start_date,
        ROW_NUMBER() over (
            partition by device_code
            order by task_start_date desc
        ) as rn

    from passages_appro

    where device_code is not null

),

-- ============================================================
-- CTE 3 : Contexte machine retenu
-- Objectif :
--   - Conserver une seule ligne par machine
--   - Garder le contexte du dernier passage appro connu
-- ============================================================
machine_context as (

    select
        device_code,
        device_name,
        device_label,
        company_code,
        company_name,
        company_label,
        roadman_code,
        gea_code

    from machine_context_ranked

    where rn = 1

),

-- ============================================================
-- CTE 4 : Dernier passage appro réalisé
-- Objectif :
--   - Identifier le dernier passage appro fait pour chaque machine
-- ============================================================
last_appro_done as (

    select
        device_code,
        task_start_date as last_appro_date,
        ROW_NUMBER() over (
            partition by device_code
            order by task_start_date desc
        ) as rn

    from passages_appro

    where
        is_done = 1
        and device_code is not null

),

-- ============================================================
-- CTE 5 : Dernier passage appro réalisé retenu
-- Objectif :
--   - Garder uniquement le passage appro réalisé le plus récent
-- ============================================================
last_appro_done_final as (

    select
        device_code,
        last_appro_date

    from last_appro_done

    where rn = 1

),

-- ============================================================
-- CTE 6 : Prochain passage appro prévu
-- Objectif :
--   - Identifier le prochain passage appro planifié pour chaque machine
-- ============================================================
next_appro_planned as (

    select
        device_code,
        task_start_date as next_appro_date,
        ROW_NUMBER() over (
            partition by device_code
            order by task_start_date asc
        ) as rn

    from passages_appro

    where
        is_planned = 1
        and DATE(task_start_date) >= CURRENT_DATE()
        and device_code is not null

),

-- ============================================================
-- CTE 7 : Prochain passage appro prévu retenu
-- Objectif :
--   - Garder uniquement le passage planifié le plus proche
-- ============================================================
next_appro_planned_final as (

    select
        device_code,
        next_appro_date

    from next_appro_planned

    where rn = 1

),

-- ============================================================
-- CTE 8 : Préparation des interventions curatives Yuman
-- Objectif :
--   - Garder uniquement les curatives NESHU
--   - Nettoyer le numéro de série pour le lien avec les machines appro
--   - Construire les flags : récente, en cours ou future
-- ============================================================
clean_workorders as (

    select
        material_id,
        material_serial_number,
        UPPER(TRIM(REPLACE(material_serial_number, 'NESH_', ''))) as serial_clean,
        workorder_id as intervention_id,

        -- Colonnes utiles au dashboard
        ANY_VALUE(demand_description) as demand_description,
        ANY_VALUE(demand_created_at) as demand_created_at,
        ANY_VALUE(demand_category_name) as demand_category_name,
        ANY_VALUE(workorder_title) as intervention_title,
        ANY_VALUE(workorder_report) as intervention_report,
        ANY_VALUE(workorder_technician_name) as intervention_technician_name,
        ANY_VALUE(client_category) as client_category,
        ANY_VALUE(material_brand) as material_brand,
        ANY_VALUE(material_description) as material_description,
        ANY_VALUE(technician_equipe) as technician_equipe,

        -- Colonnes techniques utilisées pour qualifier les interventions
        ANY_VALUE(demand_status) as demand_status,
        ANY_VALUE(workorder_status) as intervention_status,
        MAX(date_done) as date_done,
        MIN(date_planned) as date_planned,

        -- Intervention curative réalisée sur les 15 derniers jours
        case
            when
                MAX(date_done) is not null
                and DATE(MAX(date_done)) between
                DATE_SUB(CURRENT_DATE(), interval 15 day)
                and CURRENT_DATE()
                then 1
            else 0
        end as is_past_intervention_15d,

        -- Intervention curative actuellement en cours
        case
            when
                ANY_VALUE(workorder_status) = 'In progress'
                and ANY_VALUE(demand_status) = 'Accepted'
                then 1
            else 0
        end as is_current_intervention,

        -- Intervention curative planifiée dans le futur
        case
            when
                ANY_VALUE(workorder_status) = 'Scheduled'
                and ANY_VALUE(demand_status) = 'Accepted'
                and MIN(date_planned) is not null
                and MIN(date_planned) > CURRENT_TIMESTAMP()
                then 1
            else 0
        end as is_future_intervention

    from {{ ref('int_yuman__demands_workorders_enriched') }}

    where
        material_serial_number is not null
        and UPPER(TRIM(partner_name)) = 'NESHU'
        and UPPER(TRIM(demand_category_name)) like 'CURATIVE%'

    group by
        material_id,
        material_serial_number,
        serial_clean,
        workorder_id

),

-- ============================================================
-- CTE 9 : Dernière intervention curative sur les 15 derniers jours
-- Objectif :
--   - Classer les interventions récentes par machine
--   - Préparer la conservation de la plus récente
-- ============================================================
past_intervention_15d as (

    select
        serial_clean,
        intervention_id as past_intervention_id,
        date_done as last_intervention_date_15d,
        demand_description as past_demand_description,
        demand_created_at as past_demand_created_at,
        demand_category_name as past_demand_category_name,
        intervention_title as past_intervention_title,
        intervention_report as past_intervention_report,
        intervention_technician_name as past_intervention_technician_name,
        client_category as past_client_category,
        material_brand as past_material_brand,
        material_description as past_material_description,
        technician_equipe as past_technician_equipe,
        ROW_NUMBER() over (
            partition by serial_clean
            order by date_done desc
        ) as rn

    from clean_workorders

    where is_past_intervention_15d = 1

),

-- ============================================================
-- CTE 10 : Dernière intervention curative récente retenue
-- Objectif :
--   - Garder la dernière intervention curative sur 15 jours par machine
-- ============================================================
past_intervention_15d_final as (

    select
        serial_clean,
        past_intervention_id,
        last_intervention_date_15d,
        past_demand_description,
        past_demand_created_at,
        past_demand_category_name,
        past_intervention_title,
        past_intervention_report,
        past_intervention_technician_name,
        past_client_category,
        past_material_brand,
        past_material_description,
        past_technician_equipe

    from past_intervention_15d

    where rn = 1

),

-- ============================================================
-- CTE 11 : Nombre d’interventions récentes par machine
-- Objectif :
--   - Compter les curatives réalisées sur les 15 derniers jours
-- ============================================================
past_intervention_count_15d as (

    select
        serial_clean,
        COUNT(distinct intervention_id) as nb_interventions_15j

    from clean_workorders

    where is_past_intervention_15d = 1

    group by serial_clean

),

-- ============================================================
-- CTE 12 : Intervention curative actuellement en cours
-- Objectif :
--   - Classer les interventions en cours par machine
--   - Préparer la conservation de la plus récente
-- ============================================================
current_intervention as (

    select
        serial_clean,
        intervention_id as current_intervention_id,
        demand_description as current_demand_description,
        demand_created_at as current_demand_created_at,
        demand_category_name as current_demand_category_name,
        intervention_title as current_intervention_title,
        intervention_report as current_intervention_report,
        intervention_technician_name as current_intervention_technician_name,
        date_done as current_date_done,
        client_category as current_client_category,
        material_brand as current_material_brand,
        material_description as current_material_description,
        technician_equipe as current_technician_equipe,
        ROW_NUMBER() over (
            partition by serial_clean
            order by demand_created_at desc
        ) as rn

    from clean_workorders

    where is_current_intervention = 1

),

-- ============================================================
-- CTE 13 : Intervention curative en cours retenue
-- Objectif :
--   - Garder une seule intervention en cours par machine
-- ============================================================
current_intervention_final as (

    select
        serial_clean,
        current_intervention_id,
        current_demand_description,
        current_demand_created_at,
        current_demand_category_name,
        current_intervention_title,
        current_intervention_report,
        current_intervention_technician_name,
        current_date_done,
        current_client_category,
        current_material_brand,
        current_material_description,
        current_technician_equipe

    from current_intervention

    where rn = 1

),

-- ============================================================
-- CTE 14 : Prochaine intervention curative planifiée
-- Objectif :
--   - Classer les interventions futures par machine
--   - Préparer la conservation de la plus proche
-- ============================================================
future_intervention as (

    select
        serial_clean,
        intervention_id as future_intervention_id,
        date_planned as future_intervention_planned_date,
        demand_description as future_demand_description,
        demand_created_at as future_demand_created_at,
        demand_category_name as future_demand_category_name,
        intervention_title as future_intervention_title,
        intervention_report as future_intervention_report,
        intervention_technician_name as future_intervention_technician_name,
        date_done as future_date_done,
        client_category as future_client_category,
        material_brand as future_material_brand,
        material_description as future_material_description,
        technician_equipe as future_technician_equipe,
        ROW_NUMBER() over (
            partition by serial_clean
            order by date_planned asc
        ) as rn

    from clean_workorders

    where is_future_intervention = 1

),

-- ============================================================
-- CTE 15 : Prochaine intervention curative planifiée retenue
-- Objectif :
--   - Garder l’intervention future la plus proche par machine
-- ============================================================
future_intervention_final as (

    select
        serial_clean,
        future_intervention_id,
        future_intervention_planned_date,
        future_demand_description,
        future_demand_created_at,
        future_demand_category_name,
        future_intervention_title,
        future_intervention_report,
        future_intervention_technician_name,
        future_date_done,
        future_client_category,
        future_material_brand,
        future_material_description,
        future_technician_equipe

    from future_intervention

    where rn = 1

),

-- ============================================================
-- CTE 16 : Assemblage final du modèle
-- Objectif :
--   - Relier les machines appro aux interventions curatives
--   - Construire les indicateurs utiles au dashboard
-- ============================================================
final as (

    select
        -- Informations client / machine
        m.company_label,
        m.company_code,
        m.company_name,
        m.device_code as machine_serial_number,
        m.device_name,
        m.device_label,
        m.roadman_code,
        m.gea_code,

        -- Suivi des passages appro
        la.last_appro_date,
        na.next_appro_date,

        -- Dernière intervention curative sur les 15 derniers jours
        p15.last_intervention_date_15d,
        p15.past_demand_description,
        p15.past_demand_created_at,
        p15.past_demand_category_name,
        p15.past_intervention_title,
        p15.past_intervention_report,
        p15.past_intervention_technician_name,
        p15.past_client_category,
        p15.past_material_brand,
        p15.past_material_description,
        p15.past_technician_equipe,
        COALESCE(pc15.nb_interventions_15j, 0) as nb_interventions_15j,

        -- Intervention curative en cours
        ci.current_demand_description,
        ci.current_demand_created_at,
        ci.current_demand_category_name,
        ci.current_intervention_title,
        ci.current_intervention_report,
        ci.current_intervention_technician_name,
        ci.current_date_done,
        ci.current_client_category,
        ci.current_material_brand,
        ci.current_material_description,
        ci.current_technician_equipe,

        -- Prochaine intervention curative planifiée
        fi.future_intervention_planned_date,
        fi.future_demand_description,
        fi.future_demand_created_at,
        fi.future_demand_category_name,
        fi.future_intervention_title,
        fi.future_intervention_report,
        fi.future_intervention_technician_name,
        fi.future_date_done,
        fi.future_client_category,
        fi.future_material_brand,
        fi.future_material_description,
        fi.future_technician_equipe,

        -- Indicateurs de lecture
        case
            when p15.past_intervention_id is not null then 1
            else 0
        end as has_past_intervention_15d,

        case
            when ci.current_intervention_id is not null then 1
            else 0
        end as has_current_intervention,

        case
            when fi.future_intervention_id is not null then 1
            else 0
        end as has_future_intervention,

        -- Synthèse de la situation interventionnelle de la machine
        case
            when
                p15.past_intervention_id is not null
                and ci.current_intervention_id is not null
                and fi.future_intervention_id is not null
                then 'Passée 15j + en cours + future'

            when
                p15.past_intervention_id is not null
                and ci.current_intervention_id is not null
                then 'Passée 15j + en cours'

            when
                p15.past_intervention_id is not null
                and fi.future_intervention_id is not null
                then 'Passée 15j + future'

            when
                ci.current_intervention_id is not null
                and fi.future_intervention_id is not null
                then 'En cours + future'

            when p15.past_intervention_id is not null
                then 'Passée 15j seulement'

            when ci.current_intervention_id is not null
                then 'En cours seulement'

            when fi.future_intervention_id is not null
                then 'Future seulement'

            else 'Aucune intervention trouvée'
        end as machine_intervention_timeline

    from machine_context as m

    left join last_appro_done_final as la
        on UPPER(TRIM(m.device_code)) = UPPER(TRIM(la.device_code))

    left join next_appro_planned_final as na
        on UPPER(TRIM(m.device_code)) = UPPER(TRIM(na.device_code))

    left join past_intervention_15d_final as p15
        on UPPER(TRIM(m.device_code)) = p15.serial_clean

    left join past_intervention_count_15d as pc15
        on UPPER(TRIM(m.device_code)) = pc15.serial_clean

    left join current_intervention_final as ci
        on UPPER(TRIM(m.device_code)) = ci.serial_clean

    left join future_intervention_final as fi
        on UPPER(TRIM(m.device_code)) = fi.serial_clean

)

-- ============================================================
-- Sélection finale du modèle
-- Objectif :
--   - Exposer une table prête à être utilisée dans Power BI
-- ============================================================
select
    company_label,
    company_code,
    company_name,
    machine_serial_number,
    device_name,
    device_label,
    roadman_code,
    gea_code,
    last_appro_date,
    next_appro_date,
    last_intervention_date_15d,
    past_demand_description,
    past_demand_created_at,
    past_demand_category_name,
    past_intervention_title,
    past_intervention_report,
    past_intervention_technician_name,
    past_client_category,
    past_material_brand,
    past_material_description,
    past_technician_equipe,
    nb_interventions_15j,
    current_demand_description,
    current_demand_created_at,
    current_demand_category_name,
    current_intervention_title,
    current_intervention_report,
    current_intervention_technician_name,
    current_date_done,
    current_client_category,
    current_material_brand,
    current_material_description,
    current_technician_equipe,
    future_intervention_planned_date,
    future_demand_description,
    future_demand_created_at,
    future_demand_category_name,
    future_intervention_title,
    future_intervention_report,
    future_intervention_technician_name,
    future_date_done,
    future_client_category,
    future_material_brand,
    future_material_description,
    future_technician_equipe,
    has_past_intervention_15d,
    has_current_intervention,
    has_future_intervention,
    machine_intervention_timeline,

    -- Métadonnées dbt
    CURRENT_TIMESTAMP() as dbt_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id  -- noqa: TMP

from final
