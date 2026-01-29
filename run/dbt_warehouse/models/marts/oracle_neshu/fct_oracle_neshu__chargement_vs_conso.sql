
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__chargement_vs_conso`
      
    
    cluster by product_type, device_id

    
    OPTIONS(
      description="""Table interm\u00e9diaire calculant, pour chaque passage APPRO, les quantit\u00e9s consomm\u00e9es (t\u00e9l\u00e9metries) et charg\u00e9es, en reconstruisant les intervalles de consommation entre deux passages.\n"""
    )
    as (
      

-- -----------------------------------------------------------------------------------
-- CTE 1 : Récupération des passages APPRO avec leur passage précédent
-- Utilisation de LAG pour identifier la période sur laquelle agréger les télémetries.
-- -----------------------------------------------------------------------------------
WITH passage_avec_suivant AS (
  SELECT 
    device_id,
    task_start_date,
    rm.roadman_code AS roadman_code,
    LAG(task_start_date) OVER (
      PARTITION BY device_id 
      ORDER BY task_start_date
    ) AS date_passage_precedent
  FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks` ta
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__vehicule_roadman` rm
    ON rm.resources_vehicule_id = ta.product_source_id
  WHERE task_start_date >= '2025-01-01'
    AND task_status_code = 'FAIT'
),
-- -----------------------------------------------------------------------------------
-- CTE 2 : Agrégations des quantités consommées (télémetries)
-- Les télémetries sont prises entre le passage précédent et le passage actuel.
-- Filtre HAVING pour supprimer les lignes vides (product_type NULL & somme = 0).
-- -----------------------------------------------------------------------------------
telemetry_agg AS (
  SELECT
    pa.device_id,
    pa.task_start_date,
    p.product_type,
    COALESCE(SUM(t.telemetry_quantity), 0) AS q_consommee
  FROM passage_avec_suivant pa
  LEFT JOIN `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetry_tasks` t
    ON pa.device_id = t.device_id
    AND t.task_start_date BETWEEN 
          COALESCE(pa.date_passage_precedent, TIMESTAMP('2024-12-30 00:00:00'))
      AND pa.task_start_date
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` p
    ON t.product_id = p.product_id
  GROUP BY 1,2,3
  HAVING p.product_type IS NOT NULL OR SUM(t.telemetry_quantity) > 0
),

-- -----------------------------------------------------------------------------------
-- CTE 3 : Agrégations des quantités chargées
-- Les chargements sont associés au passage APPRO via une jointure sur la DATE.
-- -----------------------------------------------------------------------------------
chargement_agg AS (
  SELECT
    pa.device_id,
    pa.task_start_date,
    p.product_type,
    COALESCE(SUM(cm.load_quantity), 0) AS q_chargee
  FROM passage_avec_suivant pa
  LEFT JOIN `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks`  cm
    ON pa.device_id = cm.device_id
    AND DATE(pa.task_start_date) = DATE(cm.task_start_date)
  LEFT JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` p
    ON cm.product_id = p.product_id
  GROUP BY 1,2,3
  HAVING p.product_type IS NOT NULL OR SUM(cm.load_quantity) > 0
)

-- -----------------------------------------------------------------------------------
-- Final : Fusion telemetry + chargement via FULL JOIN
-- On récupère aussi roadman & date passage précédent depuis la CTE initiale.
-- -----------------------------------------------------------------------------------
SELECT
  COALESCE(t.device_id, c.device_id) AS device_id,
  DATE(COALESCE(t.task_start_date, c.task_start_date)) AS date_debut_passage_appro,
  MIN(COALESCE(t.task_start_date, c.task_start_date)) AS task_start_date_min,
  MIN(pa.date_passage_precedent) AS date_passage_precedent,
  MAX(pa.roadman_code) AS roadman_code,
  COALESCE(t.product_type, c.product_type) AS product_type,
  sum(COALESCE(t.q_consommee, 0)) AS q_consommee,
  max(COALESCE(c.q_chargee, 0)) AS q_chargee,

  -- Métadonnées dbt
  CURRENT_TIMESTAMP() as dbt_updated_at,
  '134e6390-c090-4e75-8096-0c82dd21fc2a' as dbt_invocation_id
FROM telemetry_agg t
FULL JOIN chargement_agg c
  ON t.device_id = c.device_id
  AND t.task_start_date = c.task_start_date
  AND t.product_type = c.product_type
LEFT JOIN passage_avec_suivant pa
  ON pa.device_id = COALESCE(t.device_id, c.device_id)
  AND pa.task_start_date = COALESCE(t.task_start_date, c.task_start_date)
GROUP BY
  device_id,
  date_debut_passage_appro,
  product_type
    );
  