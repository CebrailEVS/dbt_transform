{{
    config(
        materialized='table',
        description='Table intermédiaire des tâches des pointages roadman sur les débuts de journées avec dédoublon sur un même jour et même roadman en prenant le premier pointage après 3h du matin de la journée => 1 ligne = 1 tâche pointage roadman par jour'
    )
}}

WITH 
resources_roadman AS (
    SELECT 
        thr.idtask, 
        MIN(r.idresources) AS idresources_roadman, 
        MIN(r.code) AS code_roadman
    FROM {{ ref('stg_oracle_neshu__task_has_resources') }} thr
    JOIN {{ ref('stg_oracle_neshu__resources') }} r 
        ON thr.idresources = r.idresources 
       AND r.idresources_type = 2 -- Resources Roadman
    GROUP BY thr.idtask
),
pointage AS (
    SELECT
        t.idtask,
        t.iddevice,
        t.idcompany_peer AS idcompany,
        ts.code AS status_code,
        t.real_start_date AS date_pointage,
        la.code AS label_code,
        t.created_at,
        t.updated_at,
        t.extracted_at
    FROM {{ ref('stg_oracle_neshu__task') }} t
    JOIN {{ ref('stg_oracle_neshu__label_has_task') }} lht ON t.idtask = lht.idtask
    JOIN {{ ref('stg_oracle_neshu__label') }} la ON lht.idlabel = la.idlabel AND la.idlabel = 2685 -- START_DAY uniquement
    LEFT JOIN {{ ref('stg_oracle_neshu__task_status') }} ts ON t.idtask_status = ts.idtask_status
    WHERE t.idtask_type = 194 -- TASK TYPE POINTAGE
      AND t.code_status_record = '1' -- Enregistrement actif
),
pointage_resources AS (
    SELECT 
        p.*,
        r.idresources_roadman,
        r.code_roadman
    FROM pointage p
    LEFT JOIN resources_roadman r ON p.idtask = r.idtask
),
-- Un seul pointage par jour et par roadman
pointage_unique_par_jour AS (
    SELECT
        idtask,
        idcompany,
        idresources_roadman,
        code_roadman,
        status_code,
        label_code,
        DATE(date_pointage) AS date_jour,
        created_at,
        updated_at,
        extracted_at,
        MIN(CASE 
            WHEN FORMAT_TIMESTAMP('%H:%M:%S', date_pointage) >= '03:00:00' THEN date_pointage
        END) AS premier_pointage
    FROM pointage_resources
    GROUP BY idtask, idcompany, idresources_roadman, code_roadman, status_code, label_code, DATE(date_pointage), created_at, updated_at, extracted_at 
),
pointage_min_par_jour AS (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                p.idresources_roadman,
                p.date_jour
            ORDER BY
                p.premier_pointage
        ) AS rn
    FROM pointage_unique_par_jour p
)
SELECT
    idtask as task_id,
    idcompany as company_id,
    idresources_roadman as resources_roadman_id,
    code_roadman as roadman_code,
    status_code,
    label_code,
    date_jour as date_pointage_jour,
    premier_pointage as date_pointage,
    created_at,
    updated_at,
    extracted_at
FROM pointage_min_par_jour
WHERE rn = 1 and idresources_roadman IS NOT NULL -- Garde uniquement les pointages avec roadman et dédoublonne en prenant le premier pointage du jour par roadman