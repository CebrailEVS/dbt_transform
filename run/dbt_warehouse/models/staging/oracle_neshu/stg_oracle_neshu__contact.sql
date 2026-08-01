
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__contact`
      
    
    

    
    OPTIONS(
      description="""Contacts transform\u00e9s et nettoy\u00e9s depuis la base Oracle"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_contact`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcontact as int64) as idcontact,
        cast(idcompany as int64) as idcompany,

        -- Colonnes texte
        code,
        first_name,
        last_name,
        email,
        name,
        mobile,
        phone,
        qualite,
        code_status_record,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  