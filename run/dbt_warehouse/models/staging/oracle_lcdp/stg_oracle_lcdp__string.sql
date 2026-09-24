
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__string`
      
    
    

    
    OPTIONS(
      description="""Libell\u00e9s traduits de l'ERP, nettoy\u00e9s depuis la base Oracle LCDP. R\u00e9sout les idstring en texte lisible."""
    )
    as (
      

-- Clé (idstring, langage_code) : le filtre sur 'fr_FR' se fait en aval.

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_string`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idstring as int64) as idstring,

        -- Colonnes texte
        langage_code,
        text,
        context,
        var,
        comments,

        -- Booléen
        cast(cast(ihm as int64) as boolean) as is_ihm,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  