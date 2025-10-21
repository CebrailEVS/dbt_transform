
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
      
    partition by timestamp_trunc(created_at, day)
    cluster by ec_no

    
    OPTIONS(
      description="""\u00c9critures analytiques nettoy\u00e9es issues du syst\u00e8me MSSQL Sage"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_ecriturea`
),

cleaned_data as (
    select
        -- IDs et numéro convertis en BIGINT
        cast(cb_marq as int64) as cb_marq,
        cast(ec_no as int64) as ec_no,
        cast(n_analytique as int64) as n_analytique,
        cast(ea_ligne as int64) as ea_ligne,

        -- Colonnes texte
        ca_num,
        cb_createur,
        cb_creation_user,

        -- Colonnes numériques avec gestion des valeurs nulles
        ea_montant,
        ea_quantite,

        -- Timestamps harmonisés
        timestamp(cb_creation) as created_at,
        timestamp(coalesce(cb_modification, cb_creation)) as updated_at, -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data
    );
  