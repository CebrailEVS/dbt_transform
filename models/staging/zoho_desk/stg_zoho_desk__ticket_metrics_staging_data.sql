{{
    config(
        materialized='table',
        description='Temps passé par chaque ticket dans chaque statut Zoho. Sous-table dlt de ticket_metrics. Jointure : _dlt_parent_id = stg_zoho_desk__ticket_metrics._dlt_id. Note : staging = étape de statut Zoho, pas un concept dbt.'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_ticket_metrics__staging_data') }}
),

renamed as (
    select
        -- primary key (dlt internal)
        _dlt_id,

        -- foreign key to stg_zoho_desk__ticket_metrics (dlt internal)
        _dlt_parent_id,

        -- status stage
        status,

        -- duration (STRING 'HH:MM hrs' → INT64 minutes)
        safe_cast(split(replace(handled_time, ' hrs', ''), ':')[safe_offset(0)] as int64) * 60
        + safe_cast(split(replace(handled_time, ' hrs', ''), ':')[safe_offset(1)] as int64)
            as handled_time_minutes,

        -- metadata
        _dlt_list_idx

    from source
)

select * from renamed
