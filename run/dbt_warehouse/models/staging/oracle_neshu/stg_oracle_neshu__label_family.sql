
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family`
      
    
    cluster by idlabel_family

    
    OPTIONS(
      description="""Familles de labels transform\u00e9s et nettoy\u00e9s depuis la base Oracle"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_label_family`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel_family as int64) as idlabel_family,
        
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
  