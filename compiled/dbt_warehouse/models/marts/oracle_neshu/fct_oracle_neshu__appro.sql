

-- Un seul pointage par jour et par roadman
WITH pointage_unique_par_jour AS (
    SELECT
        task_id,
        company_id,
        resources_roadman_id,
        roadman_code,
        status_code,
        label_code,
        DATE(date_pointage) AS date_jour,
        created_at,
        updated_at,
        extracted_at,
        MIN(CASE 
            WHEN FORMAT_TIMESTAMP('%H:%M:%S', date_pointage) >= '03:00:00' THEN date_pointage
        END) AS premier_pointage
    FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__pointage`
    WHERE label_code = 'START_DAY'
    GROUP BY task_id, company_id, resources_roadman_id, roadman_code, status_code, label_code, DATE(date_pointage), created_at, updated_at, extracted_at 
),
pointage_min_par_jour AS (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                p.resources_roadman_id,
                p.date_jour
            ORDER BY
                p.premier_pointage
        ) AS rn
    FROM pointage_unique_par_jour p
),
Pointage_final_table as (
SELECT
    task_id as task_id,
    company_id as company_id,
    resources_roadman_id as resources_roadman_id,
    roadman_code as roadman_code,
    status_code,
    label_code,
    date_jour as date_pointage_jour,
    premier_pointage as date_pointage,
    created_at,
    updated_at,
    extracted_at
FROM pointage_min_par_jour
WHERE rn = 1 and resources_roadman_id IS NOT NULL
),
resources_roadman AS (
    SELECT 
        thr.idtask, 
        MIN(r.idresources) AS resources_roadman_id, 
        MIN(r.code) AS roadman_code
    FROM `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources` thr
    JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` r 
        ON thr.IDRESOURCES = r.IDRESOURCES 
       AND r.IDRESOURCES_TYPE = 2
    GROUP BY thr.IDTASK
),
passage_appro as (
SELECT 
  pa.task_id,
  pa.device_id,
  pa.company_id,
  pa.product_source_id,
  r.resources_roadman_id,
  pa.product_destination_id,
  pa.product_source_type,
  pa.product_destination_type,
  pa.company_code,
  c.name as company_name,
  c.name || ' - ' || pa.company_code AS company_info,
  d.code as device_code,
  d.name as device_name,
  d.name || ' - ' || d.code AS device_info,
  g.gea_code,
  r.roadman_code,
  pa.task_location_info,
  CASE 
    WHEN pa.task_status_code IN ('FAIT', 'ENCOURS') THEN 'FAIT'
    ELSE pa.task_status_code
  END AS task_status_code,
  p.date_pointage,
  p.date_pointage_jour,
  pa.task_start_date,
  pa.task_end_date,
  (pa.task_end_date - pa.task_start_date) AS passage_duration_min,
    CASE WHEN pa.task_status_code = 'FAIT' THEN 1 ELSE 0 END AS is_done,
    CASE WHEN pa.task_status_code IN ('PREVU', 'FAIT', 'ENCOURS') THEN 1 ELSE 0 END AS is_planned,
  pa.created_at,
  pa.updated_at 
FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks` pa

LEFT JOIN resources_roadman r 
  ON pa.task_id = r.idtask

LEFT JOIN `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__roadman_gea` g 
  ON g.roadman_code = r.roadman_code

LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` c 
  ON pa.company_id = c.idcompany

LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` d 
  ON pa.device_id = d.iddevice

LEFT JOIN Pointage_final_table p 
  ON p.date_pointage_jour = DATE(pa.task_start_date) 
 AND p.resources_roadman_id = r.resources_roadman_id

WHERE r.resources_roadman_id is not null
),
passage_with_metrics AS (
  SELECT 
    pa.*,
    -- Dernière task_end_date du roadman pour ce jour (statut FAIT uniquement)
    MAX(CASE WHEN pa.task_status_code = 'FAIT' THEN pa.task_end_date END) 
      OVER (
        PARTITION BY pa.resources_roadman_id, DATE(pa.task_start_date)
        ORDER BY pa.task_end_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS last_task_end_of_day,
    
    -- Utilise date_pointage si disponible, sinon première task_start_date du jour
    COALESCE(
      pa.date_pointage,
      FIRST_VALUE(pa.task_start_date) OVER (
        PARTITION BY pa.resources_roadman_id, DATE(pa.task_start_date)
        ORDER BY pa.task_start_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      )
    ) AS effective_work_start,
    
    -- Nombre de passages du roadman ce jour
    COUNT(*) OVER (
      PARTITION BY pa.resources_roadman_id, DATE(pa.task_start_date)
    ) AS daily_task_count,
    
    -- Temps moyen par passage du roadman ce jour
    AVG(TIMESTAMP_DIFF(pa.task_end_date, pa.task_start_date, MINUTE)) OVER (
      PARTITION BY pa.resources_roadman_id, DATE(pa.task_start_date)
    ) AS avg_passage_duration_day,
    
    -- Numéro de passage dans la journée
    ROW_NUMBER() OVER (
      PARTITION BY pa.resources_roadman_id, DATE(pa.task_start_date)
      ORDER BY pa.task_start_date
    ) AS passage_rank_of_day
    
  FROM passage_appro pa
),
 -- Calcul de work_duration_min
 passage_work_duration AS (
SELECT 
  *,
  -- Calcul du work_duration en minutes (brut)
  TIMESTAMP_DIFF(last_task_end_of_day, effective_work_start, MINUTE) AS work_duration_min_raw,
  
  -- Calcul du work_duration en minutes (nettoyé)
  CASE 
    WHEN TIMESTAMP_DIFF(last_task_end_of_day, effective_work_start, MINUTE) > 720 THEN NULL  -- > 12h = suspect
    WHEN TIMESTAMP_DIFF(last_task_end_of_day, effective_work_start, MINUTE) < 0 THEN NULL    -- négatif = erreur
    WHEN last_task_end_of_day IS NULL OR effective_work_start IS NULL THEN NULL
    ELSE TIMESTAMP_DIFF(last_task_end_of_day, effective_work_start, MINUTE)
  END AS work_duration_min
FROM passage_with_metrics
)
-- Final table
SELECT
  -- 1️⃣ IDs
  task_id,
  device_id,
  company_id,
  resources_roadman_id,

  -- 2️⃣ Attributs
  company_info,
  device_info,
  roadman_code,
  gea_code,
  task_status_code,

  -- 3️⃣ Dates / Temps
  date_pointage,
  date_pointage_jour,
  task_start_date,
  task_end_date,
  last_task_end_of_day,
  effective_work_start,

  -- 4️⃣ Mesures / KPI
  is_planned,
  is_done,
  passage_rank_of_day,
  daily_task_count,
  avg_passage_duration_day,
  passage_duration_min,
  work_duration_min_raw,
  work_duration_min,

  -- 5️⃣ Métadonnées
  created_at,
  updated_at,

  -- Métadonnées dbt
   CURRENT_TIMESTAMP() as dbt_updated_at,
  'f63979de-4940-4cf0-af86-2ff6947df878' as dbt_invocation_id

FROM passage_work_duration