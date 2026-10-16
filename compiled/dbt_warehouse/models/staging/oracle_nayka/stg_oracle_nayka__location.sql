

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_location`
),

cleaned_data as (
    -- Non reprises : colonnes jamais renseignées, paramétrage de l'ERP (idinstance, geocode_status,
    -- cedex_type).
    select
        cast(idlocation as int64) as idlocation,
        code,
        name,
        address1,
        address2,
        postal,
        city,
        country,
        access_info,
        phone,
        email,
        cast(latitude as float64) as latitude,
        cast(longitude as float64) as longitude,
        cast(code_status_record as int64) as code_status_record,
        cast(idcontact_modification as int64) as idcontact_modification,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data