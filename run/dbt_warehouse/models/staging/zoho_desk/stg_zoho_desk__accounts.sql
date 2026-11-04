
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__accounts`
      
    
    

    
    OPTIONS(
      description="""Comptes Zoho Desk nettoy\u00e9s = entreprises clientes. Source : prod_raw.zoho_desk_accounts Transformation : id renomm\u00e9 en account_id.\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_accounts`
),

renamed as (
    select
        -- primary key
        id as account_id,

        -- attributes
        account_name,
        created_time,
        web_url,

        -- csat scores (STRING in source → FLOAT64)
        safe_cast(customer_happiness__bad_percentage as float64) as customer_happiness__bad_percentage,
        safe_cast(customer_happiness__ok_percentage as float64) as customer_happiness__ok_percentage,
        safe_cast(customer_happiness__good_percentage as float64) as customer_happiness__good_percentage

    from source
)

select * from renamed
    );
  