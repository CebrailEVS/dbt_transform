
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_status`
      
    
    

    
    OPTIONS(
      description="""Statuts des t\u00e2ches transform\u00e9s et nettoy\u00e9s depuis la base Oracle LCDP"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_task_status`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idtask_status as int64) as idtask_status,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  