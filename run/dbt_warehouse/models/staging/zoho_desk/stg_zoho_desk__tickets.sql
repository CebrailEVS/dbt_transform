
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__tickets`
      
    
    

    
    OPTIONS(
      description="""Tickets Zoho Desk nettoy\u00e9s \u2014 table centrale de toute l'analyse. Source : prod_raw.zoho_desk_associated_tickets Transformation : id renomm\u00e9 en ticket_id. Pour les m\u00e9triques temporelles (tickets ferm\u00e9s/rouverts par mois), utiliser stg_zoho_desk__ticket_history \u2014 ne pas utiliser closed_time qui ne refl\u00e8te que la fermeture la plus r\u00e9cente.\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_associated_tickets`
),

renamed as (
    select
        -- primary key
        id as ticket_id,

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
        safe_cast(comment_count as int64) as comment_count,
        safe_cast(thread_count as int64) as thread_count,

        -- metadata
        web_url

    from source
)

select * from renamed
    );
  