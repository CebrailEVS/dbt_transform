{{
    config(
        materialized = 'table',
        description = 'Stocks théoriques Yuman normalisés depuis GCS'
    )
}}

with source as (

    select *
    from {{ source('yuman_gcs', 'ext_gcs_yuman__stock_theorique') }}

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
