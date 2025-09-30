
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
      
    
    cluster by idcompany

    
    OPTIONS(
      description="""Clients transform\u00e9s et nettoy\u00e9s depuis la base Oracle"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_company`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcompany as int64) as idcompany,
        cast(company_idcompany as int64) as company_idcompany,
        cast(idcompany_type as int64) as idcompany_type,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,
        
        -- Colonnes texte
        code,
        code_status_record,
        name,
        siret,

        -- Timestamps harmonisés
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at, -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data
    );
  