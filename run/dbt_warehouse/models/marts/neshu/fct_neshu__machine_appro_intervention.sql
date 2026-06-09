
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__machine_appro_intervention`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nVue cross-source Neshu \u00d7 Yuman par machine en appro : pour chaque\ndevice Neshu ayant au moins un passage appro, expose son contexte appro\n(dernier FAIT, prochain PR\u00c9VU, compteurs) et ses interventions curatives\nYuman dans 3 buckets (r\u00e9cente 15j, en cours, prochaine planifi\u00e9e).\n\n[COMMENT CONSTRUITE]\nJoint `int_oracle_neshu__appro_machine_context` (intermediate\nfactoris\u00e9, grain device) avec `dim_neshu__device` et\n`dim_neshu__company` (attributs display). C\u00f4t\u00e9 Yuman, filtre\n`int_yuman__demands_workorders_enriched` sur partenaire NESHU et\ncat\u00e9gorie CURATIVE*, puis dispatch en 3 buckets selon le statut\n(workorder_status + demand_status). Cross-source join sur\n`UPPER(TRIM(device_code))` = `serial_clean` (normalisation Yuman :\nstrip 'NESH_' prefix).\n\n[GRAIN]\n1 ligne par device_id Neshu (ayant \u2265 1 passage appro).\n\n[NOTES]\n\u26a0\ufe0f Jointure cross-source actuellement fragile (string match\ndevice_code \u2194 serial). Une table de mapping device Yuman \u2194 device\nOracle Neshu pourrait remplacer ce match string si besoin futur.\nDiagnostic effectu\u00e9 (2026-05-24) : 94 % des devices avec workorders\ncurative ont un match 1-1 propre (1175/1250). 6 % (75 devices) ont\n2 materials Yuman par device \u2014 pour ces cas, les attributs d\u00e9taill\u00e9s\n`past_/current_/future_*` ne refl\u00e8tent qu'un seul material (choisi\npar row_number partition sur serial_clean order by date_done desc).\nLes compteurs (`nb_interventions_15j`) restent justes car comptent\ntous les workorders distincts du serial. ROI bridge faible \u2014 pas\nprioritaire tant que le DA ne signale pas de pain m\u00e9tier.\n\nReplace l'ancienne PR #77 (Rim) qui pla\u00e7ait ce mart \u00e0 tort dans\n`marts/technique/` avec un nom au pluriel et une logique appro\nenfouie en 7 CTEs (factoris\u00e9e via l'intermediate).\n\nOBT controlled : seuls les IDs (device_id, company_id, material_id par\nbucket) + 1-3 attributs d'affichage par dim sont expos\u00e9s. Les autres\nattributs dim (brand, technician_equipe, client_category) sont\naccessibles via jointure depuis le BI (dim_technique__material,\ndim_technique__technician).\n"""
    )
    as (
      

-- ============================================================
-- fct_neshu__machine_appro_intervention
--
-- Grain : 1 ligne par device Neshu en appro (ayant ≥ 1 passage).
--
-- Objectif métier : cross-source Neshu × Yuman, pour chaque machine
-- Neshu en appro, expose son contexte appro (dernier FAIT, prochain
-- PRÉVU) + ses interventions curatives Yuman (passé 15j, en cours,
-- futur).
--
-- Sources :
--   - int_oracle_neshu__appro_machine_context (grain device, factorisé)
--   - dim_neshu__device, dim_neshu__company (attributs display)
--   - int_yuman__demands_workorders_enriched (interventions Yuman)
--
-- ⚠️ Jointure cross-source fragile : device_code Neshu = serial Yuman
-- (UPPER + TRIM + strip 'NESH_'). À remplacer par une table de mapping
-- device Yuman ↔ device Oracle Neshu dans un suivant PR.
-- ============================================================

-- ============================================================
-- CTE 1 : Contexte appro machine + attributs display Neshu
-- ============================================================
with appro_context as (

    select
        ctx.device_id,
        ctx.company_id,

        -- Display attributes via conformed dims
        d.device_code,
        c.company_code,

        -- Appro context (depuis l'intermediate)
        ctx.last_appro_task_id,
        ctx.last_appro_date,
        ctx.last_appro_roadman_id,
        ctx.last_appro_roadman_code,
        ctx.last_appro_gea_code,
        ctx.days_since_last_appro,

        ctx.next_appro_task_id,
        ctx.next_appro_date,
        ctx.next_appro_roadman_id,
        ctx.next_appro_roadman_code,
        ctx.next_appro_gea_code,
        ctx.days_until_next_appro,

        ctx.nb_appros_realises_total,
        ctx.nb_appros_planifies_a_venir,
        ctx.nb_appros_realises_30d,
        ctx.nb_appros_realises_90d
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_machine_context` as ctx
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__device` as d
        on ctx.device_id = d.device_id
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as c
        on ctx.company_id = c.company_id

),

-- ============================================================
-- CTE 2 : Interventions Yuman NESHU curatives, normalisées
-- Grain : 1 ligne par (material, workorder).
-- ⚠️ serial_clean = jointure fragile vers Neshu device_code.
--
-- Périmètre métier : uniquement les "vraies" interventions.
--   - Closed avec workorder_motif_non_intervention ou
--     workorder_detail_non_intervention renseigné = déplacement
--     sans réparation → exclus du fait (~248 lignes).
--   - In progress avec workorder_raison_mise_en_pause renseigné
--     = intervention en pause, pas active → exclus du bucket
--     current (filtré dans le flag is_current_intervention).
-- ============================================================
yuman_workorders as (

    select
        material_id,
        material_serial_number,
        upper(trim(replace(material_serial_number, 'NESH_', '')))
            as serial_clean,
        workorder_id as intervention_id,

        -- Champs métier essentiels
        any_value(demand_description) as demand_description,
        any_value(demand_created_at) as demand_created_at,
        any_value(demand_category_name) as demand_category_name,
        any_value(workorder_title) as intervention_title,
        any_value(workorder_report) as intervention_report,

        -- Technicien Yuman (FK vers dim_technique__technician.user_id)
        any_value(technician_id) as technician_id,

        -- Statuts (pour les flags)
        any_value(demand_status) as demand_status,
        any_value(workorder_status) as intervention_status,
        max(date_started) as date_started,
        max(date_done) as date_done,
        min(date_planned) as date_planned,

        -- Flags bucket
        case
            when
                max(date_done) is not null
                and date(max(date_done))
                between date_sub(current_date(), interval 15 day)
                and current_date()
                then 1
            else 0
        end as is_past_intervention_15d,
        case
            when
                any_value(workorder_status) = 'In progress'
                and any_value(demand_status) = 'Accepted'
                and any_value(workorder_raison_mise_en_pause) is null
                then 1
            else 0
        end as is_current_intervention,
        case
            when
                any_value(workorder_status) = 'Scheduled'
                and any_value(demand_status) = 'Accepted'
                and min(date_planned) is not null
                and min(date_planned) > current_timestamp()
                then 1
            else 0
        end as is_future_intervention
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__demands_workorders_enriched`
    where
        material_serial_number is not null
        and upper(trim(partner_name)) = 'NESHU'
        and upper(trim(demand_category_name)) like 'CURATIVE%'
        -- Exclure les déplacements sans réparation (Closed sans intervention réelle)
        and workorder_motif_non_intervention is null
        and workorder_detail_non_intervention is null
    group by
        material_id, material_serial_number, serial_clean, workorder_id

),

-- ============================================================
-- CTE 3 : Compteur 15j par machine
-- ============================================================
past_count_15d as (

    select
        serial_clean,
        count(distinct intervention_id) as nb_interventions_15j
    from yuman_workorders
    where is_past_intervention_15d = 1
    group by serial_clean

),

-- ============================================================
-- CTE 4 : Dernière intervention 15j par machine (row_number)
-- ============================================================
past_15d_ranked as (

    select
        serial_clean,
        material_id,
        technician_id,
        intervention_id,
        date_done,
        demand_description,
        demand_created_at,
        demand_category_name,
        intervention_title,
        intervention_report,
        row_number() over (
            partition by serial_clean
            order by date_done desc
        ) as rn
    from yuman_workorders
    where is_past_intervention_15d = 1

),

past_15d as (
    select
        serial_clean,
        material_id as past_material_id,
        technician_id as past_technician_id,
        intervention_id as past_intervention_id,
        date_done as past_intervention_date,
        demand_description as past_demand_description,
        demand_created_at as past_demand_created_at,
        demand_category_name as past_demand_category_name,
        intervention_title as past_intervention_title,
        intervention_report as past_intervention_report
    from past_15d_ranked
    where rn = 1
),

-- ============================================================
-- CTE 5 : Intervention en cours par machine
-- ============================================================
current_ranked as (

    select
        serial_clean,
        material_id,
        technician_id,
        intervention_id,
        demand_description,
        demand_created_at,
        demand_category_name,
        intervention_title,
        intervention_report,
        date_started,
        date_done,
        row_number() over (
            partition by serial_clean
            order by demand_created_at desc
        ) as rn
    from yuman_workorders
    where is_current_intervention = 1

),

current_inter as (
    select
        serial_clean,
        material_id as current_material_id,
        technician_id as current_technician_id,
        intervention_id as current_intervention_id,
        demand_description as current_demand_description,
        demand_created_at as current_demand_created_at,
        demand_category_name as current_demand_category_name,
        intervention_title as current_intervention_title,
        intervention_report as current_intervention_report,
        date_started as current_date_started,
        date_done as current_date_done
    from current_ranked
    where rn = 1
),

-- ============================================================
-- CTE 6 : Prochaine intervention planifiée par machine
-- ============================================================
future_ranked as (

    select
        serial_clean,
        material_id,
        technician_id,
        intervention_id,
        date_planned,
        demand_description,
        demand_created_at,
        demand_category_name,
        intervention_title,
        intervention_report,
        row_number() over (
            partition by serial_clean
            order by date_planned asc
        ) as rn
    from yuman_workorders
    where is_future_intervention = 1

),

future_inter as (
    select
        serial_clean,
        material_id as future_material_id,
        technician_id as future_technician_id,
        intervention_id as future_intervention_id,
        date_planned as future_intervention_planned_date,
        demand_description as future_demand_description,
        demand_created_at as future_demand_created_at,
        demand_category_name as future_demand_category_name,
        intervention_title as future_intervention_title,
        intervention_report as future_intervention_report
    from future_ranked
    where rn = 1
)

-- ============================================================
-- Assemblage final
-- ============================================================
select
    -- IDs (FKs vers dim_neshu__device, dim_neshu__company)
    ac.device_id,
    ac.company_id,

    -- Display attributes (1-3 par dim parente)
    ac.device_code,
    ac.company_code,

    -- Contexte appro
    ac.last_appro_date,
    ac.last_appro_roadman_code,
    ac.last_appro_gea_code,
    ac.days_since_last_appro,
    ac.next_appro_date,
    ac.next_appro_roadman_code,
    ac.next_appro_gea_code,
    ac.days_until_next_appro,
    ac.nb_appros_realises_total,
    ac.nb_appros_planifies_a_venir,
    ac.nb_appros_realises_30d,
    ac.nb_appros_realises_90d,

    -- Bucket : intervention curative récente (15j)
    p15.past_material_id,
    p15.past_technician_id,
    p15.past_intervention_id,
    p15.past_intervention_date,
    p15.past_demand_description,
    p15.past_demand_created_at,
    p15.past_demand_category_name,
    p15.past_intervention_title,
    p15.past_intervention_report,
    coalesce(pc15.nb_interventions_15j, 0) as nb_interventions_15j,

    -- Bucket : intervention en cours
    ci.current_material_id,
    ci.current_technician_id,
    ci.current_intervention_id,
    ci.current_demand_description,
    ci.current_demand_created_at,
    ci.current_demand_category_name,
    ci.current_intervention_title,
    ci.current_intervention_report,
    ci.current_date_started,
    ci.current_date_done,

    -- Bucket : prochaine intervention planifiée
    fi.future_material_id,
    fi.future_technician_id,
    fi.future_intervention_id,
    fi.future_intervention_planned_date,
    fi.future_demand_description,
    fi.future_demand_created_at,
    fi.future_demand_category_name,
    fi.future_intervention_title,
    fi.future_intervention_report
from appro_context as ac
-- ⚠️ Jointure cross-source fragile sur string normalisée
-- À remplacer par une table de mapping device dans un suivant PR
left join past_15d as p15
    on upper(trim(ac.device_code)) = p15.serial_clean
left join past_count_15d as pc15
    on upper(trim(ac.device_code)) = pc15.serial_clean
left join current_inter as ci
    on upper(trim(ac.device_code)) = ci.serial_clean
left join future_inter as fi
    on upper(trim(ac.device_code)) = fi.serial_clean
    );
  