-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__inter_technique_tasks` as DBT_INTERNAL_DEST
        using (

with base_task as (

    select
        -- Identifiants
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        thr.idresources as resources_id,

        -- Codes
        c.code as company_code,
        d.code as device_code,

        -- Noms
        c.name as company_name,
        d.name as device_name,

        -- Infos métier
        l.access_info as task_location_info,
        t.comments_self,
        t.comments_peer,
        t.real_start_date as task_start_date,
        t.real_end_date as task_end_date,

        -- Status
        ts.code as task_status_code,

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
        and t.idtask_type = 131
        and t.code_status_record = '1'
        and t.real_start_date is not null
        and r.idresources_type = 2 -- Ensure we only get resources type = people

        
            and t.updated_at >= (
                select max(src.updated_at) - interval 1 day
                from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__inter_technique_tasks` as src
            )
        

),

label_pivot as (

    select
        t.idtask as task_id,
        max(
            case
                when lf.code = 'Statut inter'
                    then la.code
            end
        ) as statut_inter,
        max(
            case
                when lf.code = 'Objet intervent'
                    then la.code
            end
        ) as objet_intervent,
        max(
            case
                when lf.code = 'DEVICE_CANCEL_REASON'
                    then la.code
            end
        ) as device_cancel_reason

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_task` as lht
        on t.idtask = lht.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as la
        on lht.idlabel = la.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` as lf
        on la.idlabel_family = lf.idlabel_family

    where
        1 = 1

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
    bt.device_id,
    bt.company_id,
    bt.resources_id,

    -- Codes
    bt.company_code,
    bt.device_code,

    -- Noms
    bt.company_name,
    bt.device_name,

    -- Infos métier
    bt.task_location_info,
    bt.comments_self,
    bt.comments_peer,
    bt.task_start_date,
    bt.task_end_date,
    bt.task_status_code,

    -- Labels pivotés
    lp.statut_inter,
    lp.objet_intervent,
    lp.device_cancel_reason,

    -- Timestamps techniques
    bt.updated_at,
    bt.created_at,
    bt.extracted_at

from deduped_task as bt
left join label_pivot as lp
    on bt.task_id = lp.task_id
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.task_id = DBT_INTERNAL_DEST.task_id))

    
    when matched then update set
        `task_id` = DBT_INTERNAL_SOURCE.`task_id`,`device_id` = DBT_INTERNAL_SOURCE.`device_id`,`company_id` = DBT_INTERNAL_SOURCE.`company_id`,`resources_id` = DBT_INTERNAL_SOURCE.`resources_id`,`company_code` = DBT_INTERNAL_SOURCE.`company_code`,`device_code` = DBT_INTERNAL_SOURCE.`device_code`,`company_name` = DBT_INTERNAL_SOURCE.`company_name`,`device_name` = DBT_INTERNAL_SOURCE.`device_name`,`task_location_info` = DBT_INTERNAL_SOURCE.`task_location_info`,`comments_self` = DBT_INTERNAL_SOURCE.`comments_self`,`comments_peer` = DBT_INTERNAL_SOURCE.`comments_peer`,`task_start_date` = DBT_INTERNAL_SOURCE.`task_start_date`,`task_end_date` = DBT_INTERNAL_SOURCE.`task_end_date`,`task_status_code` = DBT_INTERNAL_SOURCE.`task_status_code`,`statut_inter` = DBT_INTERNAL_SOURCE.`statut_inter`,`objet_intervent` = DBT_INTERNAL_SOURCE.`objet_intervent`,`device_cancel_reason` = DBT_INTERNAL_SOURCE.`device_cancel_reason`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`task_id`, `device_id`, `company_id`, `resources_id`, `company_code`, `device_code`, `company_name`, `device_name`, `task_location_info`, `comments_self`, `comments_peer`, `task_start_date`, `task_end_date`, `task_status_code`, `statut_inter`, `objet_intervent`, `device_cancel_reason`, `updated_at`, `created_at`, `extracted_at`)
    values
        (`task_id`, `device_id`, `company_id`, `resources_id`, `company_code`, `device_code`, `company_name`, `device_name`, `task_location_info`, `comments_self`, `comments_peer`, `task_start_date`, `task_end_date`, `task_status_code`, `statut_inter`, `objet_intervent`, `device_cancel_reason`, `updated_at`, `created_at`, `extracted_at`)


    