
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_type`
      
    
    

    
    OPTIONS(
      description="""Types de t\u00e2ches transform\u00e9s et nettoy\u00e9s depuis la base Oracle"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_task_type`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idtask_type as int64) as idtask_type,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data
    );
  