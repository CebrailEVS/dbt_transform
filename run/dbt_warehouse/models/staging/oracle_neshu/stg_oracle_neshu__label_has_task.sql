
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task`
      
    
    cluster by idlabel

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_label_has_task`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idtask as int64) as idtask,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data
    );
  