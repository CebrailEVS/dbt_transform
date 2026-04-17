{{ config(
    materialized='table',
    description='Dimension techniciens Yuman, crée à partir de la table stg_yuman__users et stg_yuman__storehouses. Un technicien est défini comme un utilisateur ayant le user_type "technician" ou "manager" avec is_manager_as_technician à true.',
) }}

with source as (
    select * from {{ ref('stg_yuman__users') }}
),

technicians as (
    select *
    from source
    where
        user_type = 'technician'
        or (user_type = 'manager' and is_manager_as_technician = true)
),

storehouses as (
    select * from {{ ref('stg_yuman__storehouses') }}
),

final as (
    select
        -- ids
        t.user_id,
        t.nomad_id,
        t.manager_id,

        -- attributes
        t.user_name,
        t.user_email,
        t.user_phone,
        t.user_type,
        t.user_secteur,
        m.user_name as manager_name,
        s.storehouses_name,

        -- flags
        t.is_active,
        t.is_manager_as_technician,

        -- metadata
        t.created_at,
        t.updated_at
    from technicians as t
    left join source as m
        on t.manager_id = m.user_id
    left join storehouses as s
        on t.user_id = s.storehouses_id
)

select * from final
