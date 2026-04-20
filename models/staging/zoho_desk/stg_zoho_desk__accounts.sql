{{
    config(
        materialized='table',
        description='Comptes Zoho Desk nettoyés (entreprises clientes). Renomme id en account_id.'
    )
}}

with source as (
    select * from {{ source('zoho_desk', 'zoho_desk_accounts') }}
),

renamed as (
    select
        -- primary key
        id                                    as account_id,

        -- attributes
        account_name,
        created_time,
        web_url,

        -- csat scores (STRING in source → FLOAT64)
        SAFE_CAST(customer_happiness__bad_percentage  AS FLOAT64) as customer_happiness__bad_percentage,
        SAFE_CAST(customer_happiness__ok_percentage   AS FLOAT64) as customer_happiness__ok_percentage,
        SAFE_CAST(customer_happiness__good_percentage AS FLOAT64) as customer_happiness__good_percentage

    from source
)

select * from renamed
