
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_lcdp__passage_appro`
      
    partition by timestamp_trunc(task_start_date, day)
    cluster by company_id, device_id, roadman_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Passages APPRO des roadmen chez les clients LCDP, enrichis du pointage d\u00e9but/fin de journ\u00e9e du roadman.\n[COMMENT CONSTRUITE] Issu de int_oracle_lcdp__appro_tasks_enriched (t\u00e2ches appro idtask_type=32 + roadman type 2 + r\u00e9f\u00e9rentiels company/device, statut ENCOURS reclass\u00e9 FAIT) joint au pointage journalier d\u00e9riv\u00e9 de int_oracle_lcdp__pointage_tasks : date_pointage_debut = premier START_DAY \u2265 03:00 par roadman/jour, date_pointage_fin = dernier END_DAY post\u00e9rieur au d\u00e9but le m\u00eame jour.\n[GRAIN] 1 ligne par task_id (1 t\u00e2che APPRO).\n[NOTES] P\u00e9rim\u00e8tre restreint aux statuts PREVU / FAIT / ANOMALIE (ENCOURS d\u00e9j\u00e0 repli\u00e9 en FAIT en interm\u00e9diaire) ; ANNULE et VALIDE sont exclus au niveau de ce mart (l'interm\u00e9diaire conserve le p\u00e9rim\u00e8tre complet). is_planned (PREVU/FAIT/ENCOURS) / is_done (FAIT/ENCOURS) / is_anomaly (ANOMALIE) : indicateurs binaires. ANOMALIE est exclu de is_planned (donc du d\u00e9nominateur du taux, r\u00e8gle m\u00e9tier non fig\u00e9e) mais les lignes anomalie restent pr\u00e9sentes et identifiables via is_anomaly. Taux de r\u00e9alisation = sum(is_done)/sum(is_planned). pointage_missing_flag=1 si aucun d\u00e9but de journ\u00e9e trouv\u00e9 pour le roadman ce jour. passage_duration_min calcul\u00e9e uniquement pour les passages FAIT.\n"""
    )
    as (
      

-- ============================================================
-- CTE 1 : Début de journée par roadman/jour
-- Premier pointage START_DAY à partir de 03:00:00
-- ============================================================
with pointage_debut as (
    select
        resources_roadman_id,
        roadman_code,
        date_pointage_jour,
        min(case
            when
                label_code = 'START_DAY'
                and format_timestamp('%H:%M:%S', date_pointage) >= '03:00:00'
                then date_pointage
        end) as date_pointage_debut
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__pointage_tasks`
    group by resources_roadman_id, roadman_code, date_pointage_jour
),

-- ============================================================
-- CTE 2 : Fin de journée = dernier END_DAY postérieur au début
-- (évite une fin antérieure au début le même jour)
-- ============================================================
pointage_final as (
    select
        d.resources_roadman_id,
        d.roadman_code,
        d.date_pointage_jour,
        d.date_pointage_debut,
        max(case
            when
                pf.label_code = 'END_DAY'
                and pf.date_pointage >= d.date_pointage_debut
                then pf.date_pointage
        end) as date_pointage_fin
    from pointage_debut as d
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__pointage_tasks` as pf
        on
            d.resources_roadman_id = pf.resources_roadman_id
            and d.date_pointage_jour = pf.date_pointage_jour
    group by
        d.resources_roadman_id,
        d.roadman_code,
        d.date_pointage_jour,
        d.date_pointage_debut
),

-- ============================================================
-- CTE 3 : Tâches appro enrichies + pointage début/fin joint
-- ============================================================
passage_appro as (
    select
        e.*,
        p.date_pointage_debut,
        p.date_pointage_fin,
        p.date_pointage_jour,
        case when p.date_pointage_debut is null then 1 else 0 end as pointage_missing_flag
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__appro_tasks_enriched` as e
    left join pointage_final as p
        on
            e.start_date_day = p.date_pointage_jour
            and e.resources_roadman_id = p.resources_roadman_id
)

-- ============================================================
-- Sélection finale (grain : 1 ligne par task_id)
-- ============================================================
select
    -- 1️⃣ IDs
    task_id,
    device_id,
    company_id,
    resources_roadman_id,
    product_source_id,
    product_destination_id,

    -- 2️⃣ Attributs
    company_code,
    company_name,
    company_info,
    device_code,
    device_name,
    device_info,
    roadman_code,
    roadman_name,
    product_source_type,
    product_destination_type,
    task_location_info,
    task_status_code,

    -- 3️⃣ Dates / Temps
    date_pointage_debut,
    date_pointage_fin,
    start_date_day,
    task_start_date,
    task_end_date,

    -- 4️⃣ Mesures / Flags
    passage_duration_min,
    is_planned,
    is_done,
    is_anomaly,
    pointage_missing_flag,

    -- 5️⃣ Métadonnées
    created_at,
    updated_at,
    current_timestamp() as dbt_updated_at,
    'c4ff7239-e9bf-40a9-ac07-a61dbb18b47d' as dbt_invocation_id  -- noqa: TMP

from passage_appro
-- Périmètre du rapport : PREVU / FAIT (ENCOURS déjà replié en FAIT) + ANOMALIE en flag.
-- ANNULE / VALIDE exclus. ANOMALIE reste hors du taux via is_planned (défini en intermédiaire).
where task_status_code in ('PREVU', 'FAIT', 'ANOMALIE')
    );
  