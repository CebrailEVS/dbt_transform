
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location`
      
    
    cluster by idlocation

    
    OPTIONS(
      description="""Locations transform\u00e9s et nettoy\u00e9s depuis la base Oracle"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_location`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlocation as int64) as idlocation,
        
        -- Colonnes texte
        name,
        access_info,
        address1,
        address2,
        address3,
        postal,
        city,
        country,
        code,
        code_status_record,
        longitude,
        latitude,
        altitude,

        -- Dates et timestamps
        timestamp(localisation_date) as localisation_date,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data
    );
  