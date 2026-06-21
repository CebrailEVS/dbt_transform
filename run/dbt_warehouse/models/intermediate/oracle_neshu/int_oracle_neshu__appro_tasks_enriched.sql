
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks_enriched`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Passages d'approvisionnement enrichis : t\u00e2che appro + roadman affect\u00e9, r\u00e9f\u00e9rentiels company/device/GEA, statut normalis\u00e9 et dur\u00e9e du passage. Base de fct_neshu__passage_appro (analyse roadman/jour avec pointage) et des faits d\u00e9riv\u00e9s au grain machine.\n[COMMENT CONSTRUITE] int_oracle_neshu__appro_tasks enrichi du roadman (stg_oracle_neshu__task_has_resources \u00d7 resources type=2 = PERSON), du GEA (ref_oracle_neshu__roadman_gea) et des libell\u00e9s company/device. Statut normalis\u00e9 : ENCOURS reclass\u00e9 en FAIT (validation m\u00e9tier). Dur\u00e9e du passage calcul\u00e9e (fin \u2212 d\u00e9but) pour les t\u00e2ches FAIT.\n[GRAIN] 1 ligne par task_id (passage appro). ~555k lignes.\n[NOTES] Filtr\u00e9 aux passages ayant un roadman rattach\u00e9 (resources_roadman_id non NULL) \u2014 ~327 passages sans roadman exclus vs int_oracle_neshu__appro_tasks. Pas de v\u00e9hicule ici : seul le roadman (resources type=2) est joint.\n"""
    )
    as (
      

-- ============================================================
-- CTE 1 : Roadman associé à chaque tâche appro
-- Jointure stg_oracle_neshu__task_has_resources × resources (type=2 = PERSON)
-- ============================================================
with resources_roadman as (
    select
        thr.idtask,
        min(r.idresources) as resources_roadman_id,
        min(r.code) as roadman_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources` as thr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r
        on
            thr.idresources = r.idresources
            and r.idresources_type = 2
    group by thr.idtask
),

-- ============================================================
-- CTE 2 : Enrichissement tâche appro
-- Joint le référentiel company, device, GEA. Normalise le statut
-- (ENCOURS reclassé en FAIT après validation métier). Calcule la
-- durée brute du passage.
-- ============================================================
enriched as (
    select
        -- IDs
        pa.task_id,
        pa.device_id,
        pa.company_id,
        pa.product_source_id,
        r.resources_roadman_id,
        pa.product_destination_id,
        pa.product_source_type,
        pa.product_destination_type,

        -- Codes et libellés aplatis
        pa.company_code,
        c.name as company_name,
        c.name || ' - ' || pa.company_code as company_info,
        d.code as device_code,
        d.name as device_name,
        d.name || ' - ' || d.code as device_info,
        g.gea_code,
        r.roadman_code,
        pa.task_location_info,

        -- Statut normalisé (ENCOURS → FAIT)
        case
            when pa.task_status_code in ('FAIT', 'ENCOURS') then 'FAIT'
            else pa.task_status_code
        end as task_status_code,
        case when pa.task_status_code in ('FAIT', 'ENCOURS') then 1 else 0 end as is_done,
        case
            when pa.task_status_code in ('PREVU', 'FAIT', 'ENCOURS', 'ANOMALIE') then 1 else 0
        end as is_planned,
        case when pa.task_status_code = 'ANOMALIE' then 1 else 0 end as is_anomaly,

        -- Dates
        date(pa.task_start_date) as start_date_day,
        pa.task_start_date,
        pa.task_end_date,

        -- Durée du passage
        pa.task_end_date - pa.task_start_date as passage_duration_interval,
        case
            when
                pa.task_start_date is not null
                and pa.task_end_date is not null
                and pa.task_status_code = 'FAIT'
                then
                    timestamp_diff(
                        pa.task_end_date,
                        pa.task_start_date,
                        second
                    ) / 60.0
        end as passage_duration_min,
        case
            when
                pa.task_start_date is not null
                and pa.task_end_date is not null
                and pa.task_status_code = 'FAIT'
                then
                    timestamp_diff(
                        pa.task_end_date,
                        pa.task_start_date,
                        second
                    ) / 3600.0
        end as passage_duration_hours,

        -- Métadonnées
        pa.created_at,
        pa.updated_at

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks` as pa

    left join resources_roadman as r
        on pa.task_id = r.idtask

    left join `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__roadman_gea` as g
        on r.roadman_code = g.roadman_code

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on pa.company_id = c.idcompany

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
        on pa.device_id = d.iddevice

    where r.resources_roadman_id is not null
)

select * from enriched
    );
  