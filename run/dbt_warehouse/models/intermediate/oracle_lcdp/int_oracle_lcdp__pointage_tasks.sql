
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__pointage_tasks`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Pointages des roadmen LCDP : une ligne par pointage de d\u00e9but (START_DAY) ou de fin (END_DAY) de journ\u00e9e. Sert au suivi du temps de travail / amplitude des tourn\u00e9es roadman.\n[COMMENT CONSTRUITE] stg_oracle_lcdp__task (idtask_type = 194 POINTAGE, code_status_record = '1'), filtr\u00e9 aux labels START_DAY / END_DAY (idlabel 2685 / 2686), rattach\u00e9 au roadman via task_has_resources \u00d7 resources (type 2 = PERSON). Conserv\u00e9 uniquement si un roadman est rattach\u00e9.\n[GRAIN] 1 ligne par task_id (un pointage). Une journ\u00e9e de roadman = typiquement 2 pointages (un START_DAY + un END_DAY).\n[NOTES] Pas de notion de produit ni de flux. Pour reconstruire une amplitude de journ\u00e9e, apparier START_DAY et END_DAY d'un m\u00eame roadman sur date_pointage_jour.\n"""
    )
    as (
      

with
-- Une tâche peut porter plusieurs ressources d'un même type. On retient UNE
-- ligne entière (celle du plus petit idresources) pour que l'identifiant et le
-- code désignent toujours la même ressource : des min() indépendants par
-- colonne les dissociaient dès qu'il y avait deux affectations.
resources_roadman_ranked as (
    select
        thr.idtask,
        r.idresources as idresources_roadman,
        r.code as code_roadman,
        row_number() over (partition by thr.idtask order by r.idresources) as rn
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_resources` as thr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources` as r on thr.idresources = r.idresources
    where r.idresources_type = 2 -- roadman
),

resources_roadman as (
    select
        idtask,
        idresources_roadman,
        code_roadman
    from resources_roadman_ranked
    where rn = 1
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
    -- Filtre sur le code du label, pas sur son id : les ids sont propres à chaque instance Oracle.
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as la on lht.idlabel = la.idlabel and la.code in ('START_DAY', 'END_DAY')
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
    date(date_pointage) as date_pointage_jour,
    created_at,
    updated_at,
    extracted_at
from pointage_resources
where idresources_roadman is not null
    );
  