{{
    config(
        materialized='table',
        description='Table pont agent ↔ département, dénormalisée depuis le tableau JSON associated_department_ids de API Zoho. Pas de colonne id à renommer dans cette table.'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_agents__associated_department_ids') }}
),

renamed as (
    select
        -- dlt internal pk
        _dlt_id,

        -- fk to stg_zoho_desk__agents (jointure via _dlt_id, pas via agent_id)
        _dlt_parent_id,

        -- fk to stg_zoho_desk__departments (c'est un ID Zoho métier)
        value               as department_id,

        -- position dans le tableau JSON d'origine (utile pour le debug)
        _dlt_list_idx

    from source
)

select * from renamed
