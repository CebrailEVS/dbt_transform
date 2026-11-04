
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__departments`
      
    
    

    
    OPTIONS(
      description="""D\u00e9partements Zoho Desk nettoy\u00e9s = groupes organisationnels recevant les tickets. Source : prod_raw.zoho_desk_departments Transformation : id renomm\u00e9 en department_id.\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_departments`
),

renamed as (
    select
        -- primary key
        id as department_id,

        -- attributes
        name,
        description,
        sanitized_name,
        name_in_customer_portal,
        created_time,
        creator_id,
        chat_status,

        -- flags
        is_enabled,
        is_default,
        is_assign_to_team_enabled,
        is_visible_in_customer_portal,
        has_logo

    from source
)

select * from renamed
    );
  