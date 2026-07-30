
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label`
      
    
    

    
    OPTIONS(
      description="""Labels transform\u00e9s et nettoy\u00e9s depuis la base Oracle LCDP"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_label`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idlabel_family as int64) as idlabel_family,

        -- Colonnes texte
        code,

        -- Bolean
        cast(cast(system as int64) as boolean) as is_system,
        cast(cast(enabled as int64) as boolean) as is_enabled,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  