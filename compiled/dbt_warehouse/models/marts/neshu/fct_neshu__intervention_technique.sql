

-- Interventions des techniciens (Yuman) pour NESHU. Tout ce qui est affiché vient de Yuman ;
-- company_id, device_id et modele_equipement_id relient seulement aux dims Neshu (Distrilog), pour
-- croiser avec fct_neshu__intervention_roadman.
with interventions as (
    select *
    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__demands_workorders_enriched`
    where partner_name = 'NESHU'
),

-- Statut actif du matériel et du technicien : absents du modèle enrichi.
materials as (
    select
        material_id,
        material_is_active
    from `evs-datastack-prod`.`prod_marts`.`dim_technique__material`
),

technicians as (
    select
        user_id,
        is_active
    from `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`
),

devices as (
    select
        device_id,
        device_code
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`
),

-- Modèle de la machine : device.idmodel, absent de dim_neshu__device.
device_models as (
    select
        iddevice as device_id,
        idmodel as modele_equipement_id
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device`
),

-- Le code EVS existe deux fois dans Distrilog (société interne et fournisseur). Le client Yuman
-- NESH_EVS (agences et ateliers) est la société interne, EVS PROFESSIONNEL FRANCE.
companies as (
    select
        company_id,
        company_code
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`
    where company_code != 'EVS' or company_type_id = 4 -- SOCIETY
)

select
    -- Grain
    date(i.demand_created_at, 'Europe/Paris') as demand_date,
    i.demand_id,

    -- Identifiants Yuman (dims technique)
    i.workorder_id,
    i.client_id,
    i.site_id,
    i.material_id,
    i.technician_id,

    -- Identifiants Distrilog (dims Neshu), pour le croisement avec les autres faits Neshu
    c.company_id,
    d.device_id,
    dm.modele_equipement_id,

    -- Codes et noms (Yuman), du client au technicien
    i.workorder_number,
    i.client_code,
    -- Yuman suffixe le nom du client par sa catégorie (« … - PREMIUM ») : elle a sa propre colonne.
    if(
        nullif(i.client_category, '') is null,
        i.client_name,
        regexp_replace(i.client_name, concat(r'\s*-\s*', i.client_category, r'\s*$'), '')
    ) as client_name,
    nullif(i.client_category, '') as client_category,
    i.client_is_active as is_client_active,
    i.site_code,
    i.site_name,
    i.site_postal_code,
    i.material_serial_number,
    i.material_name,
    i.material_category,
    i.material_brand,
    m.material_is_active as is_material_active,
    i.workorder_technician_name,
    i.technician_equipe,
    i.technician_agency_stock,
    t.is_active as is_technician_active,

    -- Intervention
    i.intervention_state,
    -- Yuman garde les champs de pause après la reprise : couvre les pauses passées et en cours.
    i.is_workorder_paused as has_been_paused,
    i.demand_status,
    i.demand_category_name,
    i.workorder_status,
    i.workorder_category,
    i.workorder_type,

    -- Textes
    i.demand_description,
    i.demand_reject_comment,
    i.workorder_report,
    i.workorder_motif_non_intervention,
    i.workorder_detail_non_intervention,
    i.workorder_raison_mise_en_pause,
    i.workorder_explication_mise_en_pause,

    -- Chronologie (heure de Paris)
    timestamp(datetime(i.demand_created_at, 'Europe/Paris')) as demand_created_at,
    i.workorder_date_creation,
    timestamp(datetime(i.date_planned, 'Europe/Paris')) as date_planned,
    timestamp(datetime(i.date_started, 'Europe/Paris')) as date_started,
    timestamp(datetime(i.date_done, 'Europe/Paris')) as date_done,
    date(i.date_done, 'Europe/Paris') as intervention_date,

    -- Métadonnées
    timestamp(datetime(i.demand_updated_at, 'Europe/Paris')) as demand_updated_at

from interventions as i
left join materials as m
    on i.material_id = m.material_id
left join technicians as t
    on i.technician_id = t.user_id
left join devices as d
    on i.material_serial_number = concat('NESH_', d.device_code)
left join device_models as dm
    on d.device_id = dm.device_id
left join companies as c
    on i.client_code = concat('NESH_', c.company_code)