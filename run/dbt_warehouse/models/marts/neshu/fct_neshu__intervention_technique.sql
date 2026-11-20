
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
      
    
    cluster by client_id, material_id, technician_id, device_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Interventions des techniciens NESHU, g\u00e9r\u00e9es dans Yuman : curatives, pr\u00e9ventives, installations, d\u00e9sinstallations et travaux d'atelier. Une ligne par demande d'intervention, quel que soit son \u00e9tat (r\u00e9alis\u00e9e, planifi\u00e9e, en pause, rejet\u00e9e\u2026), avec le client, le site, le mat\u00e9riel et le technicien vus par Yuman. Pendant de fct_neshu__intervention_roadman, qui porte les interventions de 1er niveau des roadmen.\n[COMMENT CONSTRUITE] int_yuman__demands_workorders_enriched filtr\u00e9 sur partner_name = 'NESHU'. Tout ce qui est affich\u00e9 vient de Yuman ; la cat\u00e9gorie client (PREMIUM, VIP\u2026) est retir\u00e9e du nom et mise dans client_category. Statut actif du mat\u00e9riel et du technicien lu dans dim_technique__material et dim_technique__technician. Les identifiants Distrilog servent seulement de pont vers les dims Neshu : machine par material_serial_number = 'NESH_' || device_code, mod\u00e8le via device.idmodel, client par client_code = 'NESH_' || company_code. Instants en heure de Paris.\n[GRAIN] 1 ligne par demand_id (demande Yuman NESHU). Environ 10 900 lignes depuis d\u00e9cembre 2023.\n[NOTES] device_id est NULL sur environ 3 % des demandes : moiti\u00e9 sans mat\u00e9riel (travaux d'atelier surtout), moiti\u00e9 avec un num\u00e9ro de s\u00e9rie Yuman qui ne correspond \u00e0 aucune machine Distrilog (saisie sans pr\u00e9fixe NESH_ ou machine absente de Distrilog). company_id est NULL pour quelques codes clients absents de Distrilog (environ 1 %). Le client interne NESH_EVS (agences et ateliers, environ 470 demandes, surtout des remises en \u00e9tat) est rattach\u00e9 \u00e0 EVS PROFESSIONNEL FRANCE : \u00e0 exclure pour une analyse des clients externes. Le lien avec une escalade roadman pr\u00e9cise n'est pas fait : les deux faits se croisent par device_id, company_id et la date. Les instants sont en heure de Paris, alors que fct_neshu__workorder_delai sort les m\u00eames instants Yuman en UTC (1 \u00e0 2 h d'\u00e9cart). Statuts actifs, \u00e9quipe et agence du technicien sont ceux d'aujourd'hui, pas ceux du jour de l'intervention.\n"""
    )
    as (
      

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
    );
  