

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