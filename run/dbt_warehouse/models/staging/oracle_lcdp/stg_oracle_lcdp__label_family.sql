
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family`
      
    
    

    
    OPTIONS(
      description="""Familles de labels transform\u00e9s et nettoy\u00e9s depuis la base Oracle LCDP"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_label_family`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel_family as int64) as idlabel_family,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  