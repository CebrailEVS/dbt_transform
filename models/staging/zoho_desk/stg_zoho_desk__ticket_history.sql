{{
    config(
        materialized='table',
        description='Journal d audit par ticket — une ligne par événement (création, changement de statut, assigné, etc.). Source de vérité pour les métriques temporelles. Pas de colonne id à renommer : la PK est _dlt_id (clé interne dlt).'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_ticket_history') }}
),

renamed as (
    select
        -- primary key (dlt internal — exposé pour jointure vers stg_zoho_desk__ticket_history_event_info._dlt_parent_id)
        _dlt_id,

        -- foreign key
        _zoho_desk_associated_tickets_id,

        -- event (event_name et event_time restent ensemble — ils définissent l'événement)
        event_name,
        event_time,

        -- source
        source,

        -- actor
        actor__id,
        actor__name,
        actor__type

    from source
)

select * from renamed
