{{
    config(
        materialized='table',
        description='Threads Zoho Desk nettoyés — un échange (email/chat) par ligne. Source : zoho_desk_ticket_threads. Renomme id en thread_id, garde _zoho_desk_associated_tickets_id pour jointure 1:N vers tickets. Sert de base pour reconstruire la chronologie réponses agent/client (filtrer direction et author__type).'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_ticket_threads') }}
),

renamed as (
    select
        -- primary key
        id as thread_id,

        -- foreign key (kept as-is, like ticket_history)
        _zoho_desk_associated_tickets_id,

        -- timestamps
        created_time,

        -- thread classification
        direction,
        channel,
        content_type,
        status,
        visibility,
        summary,
        is_description_thread,
        is_forward,
        can_reply,
        has_attach,
        safe_cast(attachment_count as int64) as attachment_count,

        -- email metadata
        from_email_address,
        `to`,
        cc,
        bcc,

        -- source / author
        source__type,
        author__id,
        author__name,
        author__email,
        author__type,
        author__first_name,
        author__last_name,
        author__photo_url,

        -- response context
        responder_id,
        responded_in,
        last_rating_icon_url

    from source
)

select * from renamed
