

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_location`
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
        -- coordonnées castées : la source les livre en texte, et le zéro de
        -- tête varie selon le format d'extraction (« -.66 » vs « -0.66 »)
        cast(longitude as float64) as longitude,
        cast(latitude as float64) as latitude,
        altitude,

        -- Dates et timestamps
        timestamp(localisation_date) as localisation_date,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data