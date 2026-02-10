
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
      
    
    

    
    OPTIONS(
      description="""Export des stocks th\u00e9oriques depuis Yuman via GCS nettoy\u00e9 et typ\u00e9"""
    )
    as (
      

with source as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`ext_gcs_yuman__stock_theorique`

),

cleaned as (

    select
        -- Clean and normalize fields
        trim(r_f_rence) as reference,
        trim(d_signation) as designation,
        -- Convert quantity to float, handling decimal commas
        cast(replace(quantit_, ',', '.') as float64) as quantite,
        nullif(trim(nom_du_stock), '') as nom_du_stock,
        -- Metadata fields
        export_date,
        _sdc_source_file,
        _sdc_source_lineno
    from source

)

select *
from cleaned
    );
  