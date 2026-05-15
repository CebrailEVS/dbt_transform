

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_threads`
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