

-- =====================================================================
-- Model: int_yuman__demands_workorders_enriched
-- Description:
--   Vue unifiée des demandes d'intervention et des interventions
--   enrichie avec les informations clients, sites, matériels et utilisateurs.
--   Ce modèle sert de base intermédiaire (int) entre les données staging
--   et les modèles marts métier.
-- =====================================================================

-- =======================
-- CTE 1 : Demandes d'intervention
-- =======================
with workorder_demands as (
    select
        demand_id,
        workorder_id,
        material_id,
        site_id,
        client_id,
        user_id,
        demand_description,
        demand_status,
        demand_reject_comment,
        created_at as demand_created_at,
        updated_at as demand_updated_at,
        demand_category_id
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands`
),

-- =======================
-- CTE 2 : Catégories de demandes
-- =======================
workorder_demands_categories as (
    select
        demand_category_id,
        demand_category_name
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands_categories`
),

-- =======================
-- CTE 3 : Interventions (workorders)
-- =======================
workorders as (
    select
        workorder_id,
        technician_id,
        workorder_number,
        workorder_category,
        workorder_type,
        workorder_status,
        workorder_title,
        workorder_description,
        workorder_report,
        workorder_technician_name,
        workorder_date_creation,
        workorder_motif_non_intervention,
        workorder_detail_non_intervention,
        workorder_raison_mise_en_pause,
        workorder_explication_mise_en_pause,
        workorder_necessite_intervenir,
        workorder_si_non_pourquoi,
        date_planned,
        date_started,
        date_done
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders`
),

-- =======================
-- CTE 4 : Clients
-- =======================
clients as (
    select
        client_id,
        partner_name,
        client_code,
        client_name,
        client_category,
        is_active as client_is_active
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__clients`
),

-- =======================
-- CTE 5 : Sites
-- =======================
sites as (
    select
        site_id,
        site_code,
        site_name,
        site_address,
        site_postal_code
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites`
),

-- =======================
-- CTE 6 : Matériels
-- =======================
materials as (
    select
        material_id,
        material_name,
        material_serial_number,
        material_brand,
        material_description,
        material_in_service_date,
        category_id
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials`
),

-- =======================
-- CTE 7 : Catégories de matériels
-- =======================
materials_categories as (
    select
        category_id,
        category_name as material_category
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories`
),

-- =======================
-- CTE 8 : Utilisateurs
-- =======================
users as (
    select
        user_id,
        manager_id,
        user_name,
        user_type,
        is_manager_as_technician
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users`
),

-- =======================
-- CTE 9 : Mapping techniciens - agences
-- =======================
tech_agence_mapping as (
    select
        nom,
        prenom,
        agence,
        equipe
    from `evs-datastack-prod`.`prod_reference`.`ref_yuman__tech_agence`
)

-- =======================
-- FINAL SELECT : Assemblage des données
-- =======================
select
    -- === Demandes d'intervention ===
    wd.demand_id,
    wd.workorder_id,
    wd.material_id,
    wd.site_id,
    wd.client_id,
    wd.user_id,
    wd.demand_description,
    wd.demand_status,
    wd.demand_reject_comment,
    wd.demand_created_at,
    wd.demand_updated_at,

    -- === Catégorie demande ===
    wdc.demand_category_name,

    -- === Interventions ===
    wo.technician_id,
    wo.workorder_number,
    wo.workorder_category,
    wo.workorder_type,
    wo.workorder_status,
    wo.workorder_title,
    wo.workorder_description,
    wo.workorder_report,
    wo.workorder_technician_name,
    wo.workorder_date_creation,
    wo.workorder_motif_non_intervention,
    wo.workorder_detail_non_intervention,
    wo.workorder_raison_mise_en_pause,
    wo.workorder_explication_mise_en_pause,
    wo.workorder_necessite_intervenir,
    wo.workorder_si_non_pourquoi,
    wo.date_planned,
    wo.date_started,
    wo.date_done,

    -- === Clients ===
    cl.partner_name,
    cl.client_code,
    cl.client_name,
    cl.client_category,
    cl.client_is_active,

    -- === Sites ===
    s.site_code,
    s.site_name,
    s.site_address,
    s.site_postal_code,

    -- === Matériels ===
    m.material_name,
    m.material_serial_number,
    m.material_brand,
    m.material_description,
    m.material_in_service_date,

    -- === Catégories de matériels ===
    mc.material_category,

    -- === Utilisateurs ===
    u.manager_id,
    u.user_name,
    u.user_type,
    u.is_manager_as_technician,

    -- === Agence du technicien (via mapping) ===
    tam.agence as technician_agency_stock,
    tam.equipe as technician_equipe

from workorder_demands wd
left join workorder_demands_categories wdc
    on wd.demand_category_id = wdc.demand_category_id

full join workorders wo
    on wd.workorder_id = wo.workorder_id

left join clients cl
    on wd.client_id = cl.client_id

left join sites s
    on wd.site_id = s.site_id

left join materials m
    on wd.material_id = m.material_id

left join materials_categories mc
    on m.category_id = mc.category_id

left join users u
    on wd.user_id = u.user_id

left join tech_agence_mapping tam
    on UPPER(TRIM(REGEXP_REPLACE(wo.workorder_technician_name, r'\[INACTIF\]\s*', ''))) = 
       UPPER(TRIM(tam.nom || ' ' || tam.prenom))