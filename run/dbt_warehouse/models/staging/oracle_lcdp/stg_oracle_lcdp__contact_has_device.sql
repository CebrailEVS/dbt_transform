
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact_has_device`
      
    
    

    
    OPTIONS(
      description="""Rattachement d'une machine au membre du personnel interne qui en a la charge (approvisionneur / technicien). Grain : 1 ligne par couple contact \u00d7 device. Le contact se raccroche \u00e0 une ressource via son `code` (aucune FK idresources dans la source). Couvre ~27 % du parc actif LCDP, mais la quasi-totalit\u00e9 du p\u00e9rim\u00e8tre DA FROID Nayax.\n"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_contact_has_device`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcontact as int64) as idcontact,
        cast(iddevice as int64) as iddevice,

        -- Timestamps harmonisés
        -- La source ne porte ni creation_date ni modification_date : pas de
        -- created_at / updated_at exposables (cf. autres tables de jonction).
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data
    );
  