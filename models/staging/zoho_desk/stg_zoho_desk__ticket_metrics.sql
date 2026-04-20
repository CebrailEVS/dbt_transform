{{
    config(
        materialized='table',
        description='Métriques SLA et compteurs par ticket (1:1 avec tickets). Durées converties de STRING HH:MM hrs en INT64 minutes. Renomme _zoho_desk_associated_tickets_id en ticket_id.'
    )
}}

-- Macro locale : convertit "HH:MM hrs" en minutes (INT64)
-- Formule : heures * 60 + minutes
-- safe_cast retourne NULL si la valeur source est NULL ou malformée

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_ticket_metrics') }}
),

renamed as (
    select
        -- primary key + foreign key to stg_zoho_desk__tickets
        _zoho_desk_associated_tickets_id as ticket_id,

        -- durations (STRING 'HH:MM hrs' → INT64 minutes)
        safe_cast(split(replace(first_response_time, ' hrs', ''), ':')[safe_offset(0)] as int64) * 60
            + safe_cast(split(replace(first_response_time, ' hrs', ''), ':')[safe_offset(1)] as int64)
            as first_response_time_minutes,

        safe_cast(split(replace(resolution_time, ' hrs', ''), ':')[safe_offset(0)] as int64) * 60
            + safe_cast(split(replace(resolution_time, ' hrs', ''), ':')[safe_offset(1)] as int64)
            as resolution_time_minutes,

        safe_cast(split(replace(total_response_time, ' hrs', ''), ':')[safe_offset(0)] as int64) * 60
            + safe_cast(split(replace(total_response_time, ' hrs', ''), ':')[safe_offset(1)] as int64)
            as total_response_time_minutes,

        -- counts (STRING → INT64)
        safe_cast(response_count as int64) as response_count,
        safe_cast(outgoing_count as int64) as outgoing_count,
        safe_cast(thread_count as int64) as thread_count,
        safe_cast(reopen_count as int64) as reopen_count,
        safe_cast(reassign_count as int64) as reassign_count,

        -- dlt internal key — exposé pour jointure vers agents_handled et staging_data
        _dlt_id

    from source
)

select * from renamed
