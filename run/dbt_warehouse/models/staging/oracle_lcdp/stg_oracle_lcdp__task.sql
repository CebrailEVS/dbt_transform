-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as DBT_INTERNAL_DEST
        using (

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_task`
),

cleaned_data as (
    select
        -- IDs (clés primaires et étrangères)
        cast(idtask as int64) as idtask,
        cast(task_idtask as int64) as task_idtask,
        cast(idtask_type as int64) as idtask_type,
        cast(idtask_status as int64) as idtask_status,
        cast(iddevice as int64) as iddevice,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(idlocation as int64) as idlocation,
        cast(idcontact as int64) as idcontact,
        cast(idproduct_source as int64) as idproduct_source,
        cast(idproduct_destination as int64) as idproduct_destination,

        -- Colonnes texte et types
        type_product_source,
        type_product_destination,
        comments_self,
        comments_peer,

        -- Colonne numérique
        spantime, -- durée de la tâche en minute
        -- status record (1 = validé, 0 ou -1 possiblement inactif/desactivé : à confirmer)
        cast(code_status_record as string) as code_status_record,

        -- Date de la tache
        timestamp(real_start_date) as real_start_date,
        timestamp(real_end_date) as real_end_date,
        timestamp(planed_start_date) as planed_start_date,
        timestamp(planed_end_date) as planed_end_date,

        -- Timestamps harmonisés
        timestamp(creation_date) as created_at,
        -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data

    where
        (
            updated_at > (
                select max(t.updated_at)
                from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
            )
            or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
        )

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.idtask = DBT_INTERNAL_DEST.idtask))

    
    when matched then update set
        `idtask` = DBT_INTERNAL_SOURCE.`idtask`,`task_idtask` = DBT_INTERNAL_SOURCE.`task_idtask`,`idtask_type` = DBT_INTERNAL_SOURCE.`idtask_type`,`idtask_status` = DBT_INTERNAL_SOURCE.`idtask_status`,`iddevice` = DBT_INTERNAL_SOURCE.`iddevice`,`idcompany_peer` = DBT_INTERNAL_SOURCE.`idcompany_peer`,`idlocation` = DBT_INTERNAL_SOURCE.`idlocation`,`idcontact` = DBT_INTERNAL_SOURCE.`idcontact`,`idproduct_source` = DBT_INTERNAL_SOURCE.`idproduct_source`,`idproduct_destination` = DBT_INTERNAL_SOURCE.`idproduct_destination`,`type_product_source` = DBT_INTERNAL_SOURCE.`type_product_source`,`type_product_destination` = DBT_INTERNAL_SOURCE.`type_product_destination`,`comments_self` = DBT_INTERNAL_SOURCE.`comments_self`,`comments_peer` = DBT_INTERNAL_SOURCE.`comments_peer`,`spantime` = DBT_INTERNAL_SOURCE.`spantime`,`code_status_record` = DBT_INTERNAL_SOURCE.`code_status_record`,`real_start_date` = DBT_INTERNAL_SOURCE.`real_start_date`,`real_end_date` = DBT_INTERNAL_SOURCE.`real_end_date`,`planed_start_date` = DBT_INTERNAL_SOURCE.`planed_start_date`,`planed_end_date` = DBT_INTERNAL_SOURCE.`planed_end_date`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`,`deleted_at` = DBT_INTERNAL_SOURCE.`deleted_at`
    

    when not matched then insert
        (`idtask`, `task_idtask`, `idtask_type`, `idtask_status`, `iddevice`, `idcompany_peer`, `idlocation`, `idcontact`, `idproduct_source`, `idproduct_destination`, `type_product_source`, `type_product_destination`, `comments_self`, `comments_peer`, `spantime`, `code_status_record`, `real_start_date`, `real_end_date`, `planed_start_date`, `planed_end_date`, `created_at`, `updated_at`, `extracted_at`, `deleted_at`)
    values
        (`idtask`, `task_idtask`, `idtask_type`, `idtask_status`, `iddevice`, `idcompany_peer`, `idlocation`, `idcontact`, `idproduct_source`, `idproduct_destination`, `type_product_source`, `type_product_destination`, `comments_self`, `comments_peer`, `spantime`, `code_status_record`, `real_start_date`, `real_end_date`, `planed_start_date`, `planed_end_date`, `created_at`, `updated_at`, `extracted_at`, `deleted_at`)


    