
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__appel_sav_tasks`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Appels SAV LCDP : une ligne par appel de service client (idtask_type 130 - SERVICECALL), avec l'agent rattach\u00e9, le statut de traitement et la cat\u00e9gorie d'appel (label FAPP01). Module r\u00e9cemment lanc\u00e9 \u2014 volum\u00e9trie volontairement faible (pr\u00e9paration du terrain).\n[COMMENT CONSTRUITE] stg_oracle_lcdp__task (idtask_type = 130, code_status_record = '1'), enrichi company / device / location et de l'agent (ressource type 2, jointure LEFT \u2014 tous les appels n'ont pas d'agent), puis pivot du label EAV exclusif FAPP01 (appel_categorie_code). D\u00e9duplication sur task_id (priorit\u00e9 au plus petit resources_id). Mat\u00e9rialis\u00e9 en table (volume faible, pas d'incr\u00e9mental).\n[GRAIN] 1 ligne par task_id (appel SAV).\n[NOTES] Le type APPEL / COMMERCIALCALL (idtask_type 201) n'a aucune t\u00e2che en source et n'est pas inclus. Les codes appel_categorie_code (APP01\u2026APP08) ne sont pas traduits dans l'entrep\u00f4t (strings Oracle non extraites) : libell\u00e9 m\u00e9tier joint depuis le seed ref_oracle_lcdp__appel_categorie. Filtre code_status_record = '1' align\u00e9 sur la fratrie int_oracle_lcdp__*_tasks (\u00e9carte les enregistrements non actifs).\n"""
    )
    as (
      

with base_task as (

    select
        -- Identifiants
        t.idtask as task_id,
        t.idcompany_peer as company_id,
        t.iddevice as device_id,
        t.idcontact as contact_id,
        t.idlocation as location_id,
        thr.idresources as resources_id,

        -- Codes / noms
        c.code as company_code,
        d.code as device_code,
        c.name as company_name,
        d.name as device_name,

        -- Infos métier
        l.access_info as task_location_info,
        t.comments_self,
        t.comments_peer,

        -- Status
        ts.code as task_status_code,

        -- Dates métier
        t.real_start_date as task_start_date,
        t.real_end_date as task_end_date,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as c
        on t.idcompany_peer = c.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as d
        on t.iddevice = d.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_resources` as thr
        on t.idtask = thr.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources` as r
        on
            thr.idresources = r.idresources
            and r.idresources_type = 2
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_status` as ts
        on t.idtask_status = ts.idtask_status
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location` as l
        on t.idlocation = l.idlocation

    where
        1 = 1
        and t.idtask_type = 130
        and t.code_status_record = '1'

),

label_pivot as (

    select
        t.idtask as task_id,
        max(
            case
                when lf.code = 'FAPP01'
                    then la.code
            end
        ) as appel_categorie_code

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_task` as lht
        on t.idtask = lht.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as la
        on lht.idlabel = la.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` as lf
        on la.idlabel_family = lf.idlabel_family

    where
        1 = 1
        and t.idtask_type = 130

    group by
        t.idtask

),

deduped_task as (

    select *
    from (
        select
            bt.*,
            row_number() over (
                partition by bt.task_id
                order by bt.resources_id
            ) as rn
        from base_task as bt
    )
    where rn = 1

)

select
    -- Identifiants
    bt.task_id,
    bt.company_id,
    bt.device_id,
    bt.contact_id,
    bt.location_id,
    bt.resources_id,

    -- Codes / noms
    bt.company_code,
    bt.device_code,
    bt.company_name,
    bt.device_name,

    -- Infos métier
    bt.task_location_info,
    bt.comments_self,
    bt.comments_peer,
    bt.task_status_code,

    -- Label pivoté
    lp.appel_categorie_code,
    cat.appel_categorie_label,

    -- Dates métier
    bt.task_start_date,
    bt.task_end_date,

    -- Timestamps techniques
    bt.updated_at,
    bt.created_at,
    bt.extracted_at

from deduped_task as bt
left join label_pivot as lp
    on bt.task_id = lp.task_id
left join `evs-datastack-prod`.`prod_reference`.`ref_oracle_lcdp__appel_categorie` as cat
    on lp.appel_categorie_code = cat.appel_categorie_code
    );
  