
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__pointage`
      
    
    

    
    OPTIONS(
      description="""Table interm\u00e9diaire des t\u00e2ches des pointages roadman sur les d\u00e9buts et fin de journ\u00e9es 1 ligne = 1 t\u00e2che pointage. Filtr\u00e9e sur les t\u00e2ches de type POINTAGE et FILTRER sur label START_DAY (idtask_type=194, idlabel=2685,2686) et filtrage sur les t\u00e2ches associ\u00e9 \u00e0 des idresources avec code_status_record=1\n"""
    )
    as (
      

with
resources_roadman as (
    select
        thr.idtask,
        MIN(r.idresources) as idresources_roadman,
        MIN(r.code) as code_roadman
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_resources` as thr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources` as r
        on
            thr.idresources = r.idresources
            and r.idresources_type = 2 -- Resources Roadman
    group by thr.idtask
),

pointage as (
    select
        t.idtask,
        t.iddevice,
        t.idcompany_peer as idcompany,
        ts.code as status_code,
        t.real_start_date as date_pointage,
        la.code as label_code,
        t.created_at,
        t.updated_at,
        t.extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_task` as lht on t.idtask = lht.idtask
    -- START_DAY & END_DAY uniquement
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as la on lht.idlabel = la.idlabel and la.idlabel in (2685, 2686)
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_status` as ts on t.idtask_status = ts.idtask_status
    where
        t.idtask_type = 194 -- TASK TYPE POINTAGE
        and t.code_status_record = '1' -- Enregistrement actif
),

pointage_resources as (
    select
        p.*,
        r.idresources_roadman,
        r.code_roadman
    from pointage as p
    left join resources_roadman as r on p.idtask = r.idtask
)

-- SELECT FINAL
select
    idtask as task_id,
    idcompany as company_id,
    idresources_roadman as resources_roadman_id,
    code_roadman as roadman_code,
    status_code,
    label_code,
    date_pointage,
    DATE(date_pointage) as date_pointage_jour,
    created_at,
    updated_at,
    extracted_at
from pointage_resources
where idresources_roadman is not null
    );
  