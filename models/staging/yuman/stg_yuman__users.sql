{{
  config(
    materialized='table',
    description='Utilisateurs nettoyés depuis l Yuman API',
  )
}}

with source_data as (
    select * from {{ source('yuman_api', 'yuman_users') }}
),

cleaned_users as (
    select
        id as user_id,
        manager_id, -- ID du manager de l'utilisateur
        agency_id,
        name as user_name,
        email as user_email,
        user_type as user_type,
        phone as user_phone,
        manager_as_technician as is_manager_as_technician,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(_sdc_extracted_at as timestamp) as extracted_at,
        cast(_sdc_deleted_at as timestamp) as deleted_at
    from source_data
)

select * from cleaned_users