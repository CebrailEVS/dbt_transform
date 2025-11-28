

with source as (
    select 
        *
    from `evs-datastack-prod`.`prod_raw`.`ext_gcs_yuman__stock_theorique`
),

cleaned as (
    select
        -- Clean and normalize fields
        trim(r_f_rence) as reference,
        trim(d_signation) as designation,
        
        -- quantit_ is already INTEGER, but may be NULL if parsing failed
        -- Convert to FLOAT to handle decimal values properly
        cast(quantit_ as float64) as quantite,
        
        trim(nom_du_stock) as nom_du_stock,
        
        -- Metadata fields
        export_date,
        _sdc_source_file,
        _sdc_source_lineno
        
    from source
)

select * from cleaned