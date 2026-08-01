
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label`
      
    
    

    
    OPTIONS(
      description="""Labels transform\u00e9s et nettoy\u00e9s depuis la base Oracle"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_label`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idlabel_family as int64) as idlabel_family,

        -- Colonnes texte
        code,

        -- Bolean
        -- NUMERIC(1) depuis le passage de prod_raw a dlt : le NUMBER(1) Oracle
        -- n'arrive plus en BOOL. Double cast, BigQuery refusant NUMERIC -> BOOL.
        cast(cast(system as int64) as boolean) as is_system,
        cast(cast(enabled as int64) as boolean) as is_enabled,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  