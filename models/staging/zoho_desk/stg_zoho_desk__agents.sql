{{
    config(
        materialized='table',
        description='Agents Zoho Desk nettoyés (membres de l équipe support). Renomme id en agent_id. Expose _dlt_id pour la jointure vers stg_zoho_desk__agent_departments.'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_agents') }}
),

renamed as (
    select
        -- primary key
        id as agent_id,

        -- identity
        first_name,
        last_name,
        name,
        email_id,

        -- role & permissions
        status,
        role_id,
        profile_id,
        role_permission_type,

        -- contact info
        phone,
        mobile,

        -- locale
        time_zone,
        lang_code,
        country_code,

        -- misc
        about_info,
        extn,
        photo_url,
        is_confirmed,
        is_zia_agent,
        zuid,

        -- dlt internal key — kept intentionally for joining to stg_zoho_desk__agent_departments
        -- jointure : stg_zoho_desk__agent_departments._dlt_parent_id = _dlt_id
        _dlt_id

    from source
)

select * from renamed
