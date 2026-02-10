{{
    config(
        materialized = 'table',
        description = 'Utilisateurs nettoyés depuis l Yuman API'
    )
}}

with source_data as (

    select *
    from {{ source('yuman_api', 'yuman_users') }}

),

cleaned_users as (

    select
        id as user_id,
        manager_id, -- id du manager de l'utilisateur
        name as user_name,
        email as user_email,
        user_type,
        phone as user_phone,
        manager_as_technician as is_manager_as_technician,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data

)

select *
from cleaned_users
