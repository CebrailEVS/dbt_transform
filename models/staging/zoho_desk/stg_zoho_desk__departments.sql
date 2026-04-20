{{
    config(
        materialized='table',
        description='Départements Zoho Desk nettoyés (groupes organisationnels recevant les tickets). Renomme id en department_id.'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_departments') }}
),

renamed as (
    select
        -- primary key
        id as department_id,

        -- attributes
        name,
        description,
        sanitized_name,
        name_in_customer_portal,
        created_time,
        creator_id,
        chat_status,

        -- flags
        is_enabled,
        is_default,
        is_assign_to_team_enabled,
        is_visible_in_customer_portal,
        has_logo

    from source
)

select * from renamed
