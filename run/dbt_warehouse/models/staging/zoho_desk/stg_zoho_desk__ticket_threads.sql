
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_threads`
      
    
    

    
    OPTIONS(
      description="""Threads (\u00e9changes email/chat) par ticket \u2014 base pour reconstituer la chronologie des r\u00e9ponses agent et client. Source : prod_raw.zoho_desk_ticket_threads Transformation : id renomm\u00e9 en thread_id, attachment_count cast STRING \u2192 INT64. Jointure vers tickets : _zoho_desk_associated_tickets_id = stg_zoho_desk__tickets.ticket_id Usage typique :\n  - Premi\u00e8re r\u00e9ponse agent : MIN(created_time) WHERE direction='out' AND author__type='AGENT'\n  - Premier message client : MIN(created_time) WHERE direction='in'\n"""
    )
    as (
      

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
    );
  