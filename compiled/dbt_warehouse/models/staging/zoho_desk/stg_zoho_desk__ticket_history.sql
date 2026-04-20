

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_history`
),

renamed as (
    select
        -- primary key (dlt internal) — jointure vers ticket_history_event_info._dlt_parent_id
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