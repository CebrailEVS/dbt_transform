{{
    config(
        materialized='table',
        description='Contact nettoyés et enrichis depuis evs_contact'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_contact') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcontact as int64) as idcontact,
        cast(idcompany as int64) as idcompany,

        -- Colonnes texte
        code,
        first_name,
        last_name,
        email,
        name,
        mobile,
        phone,
        qualite,
        code_status_record,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
