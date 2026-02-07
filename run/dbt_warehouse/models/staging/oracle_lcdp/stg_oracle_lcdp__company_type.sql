
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company_type`
      
    
    

    
    OPTIONS(
      description="""Types d'entreprises transform\u00e9s et nettoy\u00e9s depuis la base Oracle LCDP"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_company_type`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcompany_type as int64) as idcompany_type,

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
  