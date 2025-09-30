
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources`
      
    
    cluster by idtask

    
    OPTIONS(
      description="""Association entre les t\u00e2ches et les ressources"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_task_has_resources`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idtask as int64) as idtask,
        cast(idresources as int64) as idresources,
        cast(task_status as int64) as task_status,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
),

-- Synchro avec la table des tâches pour éviter les orphelins
filtered_data as (
    select cr.*
    from cleaned_data cr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` t
      on cr.idtask = t.idtask
)

select * from filtered_data
    );
  