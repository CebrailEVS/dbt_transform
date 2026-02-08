

  create or replace view `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__pa_business_review`
  OPTIONS(
      description="""Table de faits des passages appro (Business Review Neshu). Permet de tracer le passage des roadmen chez les clients \u00e0 partir de 2025, avec enrichissement des informations clients et machines.\n"""
    )
  as 

SELECT
    -- Identifiants
    pa.task_id,
    pa.company_id,
    pa.device_id,
    pa.company_code,

    -- Company
    c.company_name,
    CONCAT(c.company_name, ' - ', pa.company_code) AS company_info,

    -- Device
    d.device_brand,
    d.device_code,
    CONCAT(d.device_brand, ' - ', d.device_code) AS device_info,

    -- Contexte temporel
    pa.task_start_date,
    DATE(pa.task_start_date) AS task_start_date_day,
    pa.task_end_date,

    -- Statut
    pa.task_status_code,
    CASE WHEN pa.task_status_code = 'FAIT' THEN 1 ELSE 0 END AS mission_faite,
    CASE WHEN pa.task_status_code IN ('PREVU', 'FAIT', 'ENCOURS') THEN 1 ELSE 0 END AS mission_prevue,

    -- Métadonnées dbt
    CURRENT_TIMESTAMP() as dbt_updated_at,
    'c53bc50c-059c-4bdc-a1f2-7acc0bb3f9f9' as dbt_invocation_id

FROM `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks` pa
JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` d 
    ON pa.device_id = d.device_id
JOIN `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company` c 
    ON pa.company_id = c.company_id
WHERE DATE(pa.task_start_date) >= '2025-01-01'
    AND pa.task_status_code != 'ANNULE';

