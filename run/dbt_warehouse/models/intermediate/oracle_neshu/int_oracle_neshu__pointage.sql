
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__pointage`
      
    
    

    
    OPTIONS(
      description="""Table interm\u00e9diaire des t\u00e2ches des pointages roadman sur les d\u00e9buts et fin de journ\u00e9es  1 ligne = 1 t\u00e2che pointage. Filtr\u00e9e sur les t\u00e2ches de type POINTAGE et FILTRER sur label START_DAY (idtask_type=194, idlabel=2685,2686) et filtrage sur les t\u00e2ches associ\u00e9 \u00e0 des idresources avec code_status_record=1\n"""
    )
    as (
      

WITH 
resources_roadman AS (
    SELECT 
        thr.idtask, 
        MIN(r.idresources) AS idresources_roadman, 
        MIN(r.code) AS code_roadman
    FROM `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources` thr
    JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` r 
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
    FROM `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` t
    JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task` lht ON t.idtask = lht.idtask
    JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` la ON lht.idlabel = la.idlabel AND la.idlabel in (2685,2686)  -- START_DAY & END_DAY uniquement
    LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` ts ON t.idtask_status = ts.idtask_status
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
)
 
-- SELECT FINAL
SELECT
    idtask as task_id,
    idcompany as company_id,
    idresources_roadman as resources_roadman_id,
    code_roadman as roadman_code,
    status_code,
    label_code,
    date_pointage as date_pointage,
    DATE(date_pointage) as date_pointage_jour,
    created_at,
    updated_at,
    extracted_at
FROM pointage_resources
WHERE idresources_roadman is not null
    );
  