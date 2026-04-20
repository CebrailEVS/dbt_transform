{{
    config(
        materialized='table',
        description='Tickets Zoho Desk nettoyés — table centrale de toute lanalyse. Source : zoho_desk_associated_tickets. Renomme id en ticket_id.'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_associated_tickets') }}
),

renamed as (
    select
        -- primary key
        id                                                          as ticket_id,

        -- foreign keys
        department_id,
        assignee_id,
        contact_id,
        account_id,
        team_id,
        product_id,
        layout_id,

        -- ticket identity
        ticket_number,
        subject,
        language,

        -- status & classification (flags restent dans leur domaine)
        status,
        status_type,
        priority,
        category,
        sub_category,
        is_archived,
        is_spam,

        -- channel
        channel,
        channel_code,

        -- contact info at ticket creation
        email,
        phone,

        -- sentiment (Zoho Zia AI)
        sentiment,

        -- last thread snapshot
        last_thread,
        last_thread__channel,
        last_thread__direction,
        last_thread__is_draft,
        last_thread__is_forward,

        -- source info
        source__type,
        source__app_name,
        source__ext_id,
        source__permalink,

        -- on-hold duration (STRING, format propriétaire Zoho — différent de onhold_time qui est TIMESTAMP)
        relationship_type,
        on_hold_time,

        -- dates (all TIMESTAMP in source — no cast needed)
        created_time,
        closed_time,
        due_date,
        response_due_date,
        onhold_time,
        customer_response_time,

        -- counts (STRING in source → INT64)
        SAFE_CAST(comment_count AS INT64)                           as comment_count,
        SAFE_CAST(thread_count  AS INT64)                           as thread_count,

        -- metadata
        web_url

    from source
)

select * from renamed
