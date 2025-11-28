-- fct_oracle_neshu__machines_maintenance_tracking.sql


WITH machines_base AS (
    SELECT * FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__machines_yuman_maintenance_base`
),

-- INTERVENTIONS YUMAN (toutes, pas de filtre ici)
all_workorders AS (
    SELECT 
        wd.demand_id, 
        wd.workorder_id,
        wd.material_id, 
        wd.demand_status,
        wdc.demand_category_name, 
        wo.date_planned,
        wo.date_done, 
        wo.workorder_type, 
        wo.workorder_status
    FROM `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands` wd
    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands_categories` wdc
        ON wd.demand_category_id = wdc.demand_category_id
    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders` wo
        ON wd.workorder_id = wo.workorder_id
),

-- ENRICHISSEMENT : Ajouter les interventions AUX machines (LEFT JOIN)
workorder_enrichi AS (
    SELECT 
        mb.*,
        wo.demand_id,
        wo.workorder_id,
        wo.demand_status,
        wo.demand_category_name,
        wo.date_planned,
        wo.date_done,
        wo.workorder_type,
        wo.workorder_status
    FROM machines_base mb
    LEFT JOIN all_workorders wo
        ON mb.material_id = wo.material_id
),

-- LISTE INTERVENTION PREV DLOG
intervention_prev_dlog AS (
    SELECT 
        CONCAT('NESH_', device_code) AS device_code, 
        MAX(task_end_date) AS last_preventive_date
    FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_techinique_tasks`
    WHERE dc04 = 'DC0402'
        AND task_status_code = 'FAIT'
    GROUP BY device_code
),

-- AJOUT PREV DLOG DANS LA LISTE DES INTERVENTION YUMAN
material_enrichi AS (
    SELECT 
        we.*, 
        ipd.last_preventive_date 
    FROM workorder_enrichi we
    LEFT JOIN intervention_prev_dlog ipd
        ON we.material_serial_number = ipd.device_code
),

-- CALCUL DU RETARD PAR MACHINE
calcul_retard AS (
    SELECT
        device_id,
        material_id,
        device_code,
        material_serial_number,
        last_installation_date,
        device_name,
        company_code,
        company_name,
        client_code,
        client_name,
        client_category,
        site_postal_code,
        demand_status,
        date_planned,
        material_created_at,
        
        -- Déterminer la dernière préventive de la source 1 (Yuman)
        MAX(CASE 
            WHEN (workorder_type = 'Preventive' OR demand_category_name = 'PREVENTIVE PROG - NESHU') 
                AND workorder_status = 'Closed' 
            THEN date_done 
            ELSE NULL 
        END) OVER (PARTITION BY material_id) AS derniere_preventive_source1,
        
        -- Récupérer la préventive externe (source 2 - DLOG)
        MAX(last_preventive_date) OVER (PARTITION BY material_id) AS derniere_preventive_source2,
        
        -- Date du jour
        CURRENT_TIMESTAMP() AS today
        
    FROM material_enrichi
),

-- LOGIQUE PRINCIPALE DE CALCUL
retard_final AS (
    SELECT DISTINCT
        device_id,
        material_id,
        device_code,
        material_serial_number,
        last_installation_date,
        device_name,
        company_code,
        company_name,
        client_code,
        client_name,
        client_category,
        site_postal_code,
        demand_status,
        date_planned,
        material_created_at,
        
        -- CAS 1: Machine installée depuis moins de 13 mois
        CASE
            WHEN last_installation_date > TIMESTAMP_SUB(today, INTERVAL 395 DAY) THEN
                FALSE
            
            -- CAS 2: Machine ancienne sans aucune préventive
            WHEN derniere_preventive_source1 IS NULL AND derniere_preventive_source2 IS NULL THEN
                TRUE
            
            -- CAS 3: Machine avec préventives - vérifier si retard > 365 jours
            ELSE
                CASE
                    WHEN GREATEST(
                        COALESCE(derniere_preventive_source1, TIMESTAMP('1900-01-01')),
                        COALESCE(derniere_preventive_source2, TIMESTAMP('1900-01-01'))
                    ) < TIMESTAMP_SUB(today, INTERVAL 365 DAY)
                    THEN TRUE
                    ELSE FALSE
                END
        END AS retard_bol,
        
        -- CALCUL DU DÉLAI
        CASE
            -- CAS 1: Machine récente (< 13 mois)
            WHEN last_installation_date > TIMESTAMP_SUB(today, INTERVAL 395 DAY) THEN
                TIMESTAMP_DIFF(TIMESTAMP_ADD(last_installation_date, INTERVAL 395 DAY), today, DAY)
            
            -- CAS 2: Aucune préventive
            WHEN derniere_preventive_source1 IS NULL AND derniere_preventive_source2 IS NULL THEN
                -TIMESTAMP_DIFF(today, TIMESTAMP_ADD(last_installation_date, INTERVAL 365 DAY), DAY)
            
            -- CAS 3: Avec préventives
            ELSE
                CASE
                    WHEN TIMESTAMP_DIFF(today, GREATEST(
                        COALESCE(derniere_preventive_source1, TIMESTAMP('1900-01-01')),
                        COALESCE(derniere_preventive_source2, TIMESTAMP('1900-01-01'))
                    ), DAY) > 365
                    THEN -(TIMESTAMP_DIFF(today, GREATEST(
                        COALESCE(derniere_preventive_source1, TIMESTAMP('1900-01-01')),
                        COALESCE(derniere_preventive_source2, TIMESTAMP('1900-01-01'))
                    ), DAY) - 365)
                    ELSE 365 - TIMESTAMP_DIFF(today, GREATEST(
                        COALESCE(derniere_preventive_source1, TIMESTAMP('1900-01-01')),
                        COALESCE(derniere_preventive_source2, TIMESTAMP('1900-01-01'))
                    ), DAY)
                END
        END AS retard_delai,
        
        -- SOURCE DE LA DERNIÈRE PRÉVENTIVE
        CASE
            WHEN last_installation_date > TIMESTAMP_SUB(today, INTERVAL 395 DAY) THEN
                'aucune'
            WHEN derniere_preventive_source1 IS NULL AND derniere_preventive_source2 IS NULL THEN
                'aucune'
            WHEN derniere_preventive_source2 IS NULL OR 
                (derniere_preventive_source1 IS NOT NULL AND derniere_preventive_source1 > derniere_preventive_source2) THEN
                'yuman'
            ELSE
                'dlog'
        END AS source_last_preventive,
        
        today
        
    FROM calcul_retard
),

-- DÉDUPLICATION: Garder une seule ligne par machine
deduplique AS (
    SELECT
        * EXCEPT(today),
        ROW_NUMBER() OVER (
            PARTITION BY material_serial_number 
            ORDER BY 
                retard_bol ASC,                -- Les machines non en retard d'abord (FALSE avant TRUE)
                material_created_at DESC,      -- Si les 2 non en retard : la plus récente
                retard_delai ASC               -- Si les 2 en retard : le plus grand délai (négatif donc ASC)
        ) AS rn
    FROM retard_final
),

-- RÉSULTAT RETARD DEDUPLIQUÉ
resultat_retard AS (
    SELECT
        device_id,
        material_id,
        device_code,
        material_serial_number,
        last_installation_date,
        device_name,
        company_code,
        company_name,
        client_code,
        client_name,
        client_category,
        site_postal_code,
        retard_bol,
        retard_delai,
        source_last_preventive,
        material_created_at
    FROM deduplique
    WHERE rn = 1
),

-- DEMANDES D'INTERVENTION YUMAN OUVERTES & PLANIFIÉES
di_data AS (
    SELECT 
        wd.material_id,
        wo.date_planned,
        CASE
            WHEN wd.demand_status = 'Open' THEN 'Ouvert'
            WHEN wo.workorder_status = 'Scheduled' THEN 'Planifie'
            ELSE 'Aucune'
        END AS status_inter
    FROM `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands` wd
    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands_categories` wdc
        ON wd.demand_category_id = wdc.demand_category_id
    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders` wo
        ON wd.workorder_id = wo.workorder_id
    WHERE wdc.demand_category_name = 'PREVENTIVE PROG - NESHU' 
        AND (wd.demand_status = 'Open' OR wo.workorder_status = 'Scheduled')
),

-- ENRICHISSEMENT FINAL AVEC STATUT DES INTERVENTIONS
final AS (
    SELECT
        rr.device_id,
        rr.device_code,
        rr.device_name,
        rr.company_code,
        rr.company_name,
        rr.last_installation_date AS device_last_installation_date,
        rr.material_id,
        rr.material_serial_number,
        rr.client_code,
        rr.client_name,
        rr.client_category,
        rr.site_postal_code,
        rr.retard_bol,
        rr.retard_delai,
        rr.source_last_preventive,
        COALESCE(di.status_inter, 'Aucune') AS status_inter,
        di.date_planned,
        rr.material_created_at,

        -- Métadonnées dbt
        CURRENT_TIMESTAMP() as dbt_updated_at,
        'c06ab189-36aa-4fbd-a6d1-0e144836e601' as dbt_invocation_id

    FROM resultat_retard rr
    LEFT JOIN di_data di
        ON rr.material_id = di.material_id
)

SELECT * FROM final