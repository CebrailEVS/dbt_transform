
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__treatment_by_batch`
      
    
    

    
    OPTIONS(
      description="""Traitements par lot de l'ERP, nettoy\u00e9s depuis la base Oracle. D\u00e9code idtreatment_by_batch de stg_oracle_neshu__task_historic."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_treatment_by_batch`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idtreatment_by_batch as int64) as idtreatment_by_batch,
        cast(idstring_treatment as int64) as idstring_treatment,
        cast(idstring_comments as int64) as idstring_comments,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  