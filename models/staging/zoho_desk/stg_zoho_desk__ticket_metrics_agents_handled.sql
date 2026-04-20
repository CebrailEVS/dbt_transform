{{
    config(
        materialized='table',
        description='Agents ayant traité chaque ticket avec leur temps de traitement. Sous-table dlt de ticket_metrics. Jointure : _dlt_parent_id = stg_zoho_desk__ticket_metrics._dlt_id.'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_ticket_metrics__agents_handled') }}
),

renamed as (
    select
        -- primary key (dlt internal)
        _dlt_id,

        -- foreign key to stg_zoho_desk__ticket_metrics (dlt internal)
        _dlt_parent_id,

        -- agent
        agent_id,
        agent_name,

        -- duration (STRING 'HH:MM hrs' → INT64 minutes)
        safe_cast(split(replace(handling_time, ' hrs', ''), ':')[safe_offset(0)] as int64) * 60
            + safe_cast(split(replace(handling_time, ' hrs', ''), ':')[safe_offset(1)] as int64)
            as handling_time_minutes,

        -- metadata
        _dlt_list_idx

    from source
)

select * from renamed
