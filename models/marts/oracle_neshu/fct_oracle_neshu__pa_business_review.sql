{{ config(
    materialized='view',
    description='Table de faits des passages appro (Business Review Neshu) - permet de tracer le passage des roadmen chez les clients à partir de 2025'
) }}

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
    '{{ invocation_id }}' as dbt_invocation_id

FROM {{ ref('int_oracle_neshu__appro_tasks') }} pa
JOIN {{ ref('dim_oracle_neshu__device') }} d 
    ON pa.device_id = d.iddevice
JOIN {{ ref('dim_oracle_neshu__company') }} c 
    ON pa.company_id = c.idcompany
WHERE DATE(pa.task_start_date) >= '2025-01-01'
