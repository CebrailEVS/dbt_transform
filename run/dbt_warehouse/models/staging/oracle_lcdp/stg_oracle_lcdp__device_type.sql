
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device_type`
      
    
    

    
    OPTIONS(
      description="""Types d'\u00e9quipement transform\u00e9s et nettoy\u00e9s depuis la base Oracle LCDP : une machine se distingue de ses sous-\u00e9quipements (syst\u00e8me de paiement, t\u00e9l\u00e9m\u00e9trie\u2026)"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_device_type`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(iddevice_type as int64) as iddevice_type,

        -- Colonnes texte
        code,
        code_type,

        -- Timestamps harmonisés
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  