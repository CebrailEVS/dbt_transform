
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__contacts`
      
    
    

    
    OPTIONS(
      description="""Contacts Zoho Desk nettoy\u00e9s = personnes physiques (clients) qui ouvrent des tickets. Source : prod_raw.zoho_desk_contacts Transformation : id renomm\u00e9 en contact_id.\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_contacts`
),

renamed as (
    select
        -- primary key
        id as contact_id,

        -- identity
        first_name,
        last_name,
        email,
        phone,

        -- foreign key
        account_id,

        -- metadata
        owner_id,
        created_time,
        safe_cast(account_count as int64) as account_count,

        -- flags
        is_anonymous,
        is_end_user,
        is_spam,

        -- misc
        web_url,

        -- csat scores
        customer_happiness__bad_percentage,
        customer_happiness__ok_percentage,
        customer_happiness__good_percentage

    from source
)

select * from renamed
    );
  