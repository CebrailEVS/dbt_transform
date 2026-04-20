
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__agents`
      
    
    

    
    OPTIONS(
      description="""Agents Zoho Desk nettoy\u00e9s = membres de l'\u00e9quipe support. Source : prod_raw.zoho_desk_agents Transformation : id renomm\u00e9 en agent_id. Note : _dlt_id est conserv\u00e9 dans ce mod\u00e8le car il sert de cl\u00e9 de jointure vers stg_zoho_desk__agent_departments (jointure dlt interne, pas un ID Zoho).\n"""
    )
    as (
      

with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`zoho_desk_agents`
),

renamed as (
    select
        -- primary key
        id as agent_id,

        -- identity
        first_name,
        last_name,
        name,
        email_id,

        -- role & permissions
        status,
        role_id,
        profile_id,
        role_permission_type,

        -- contact info
        phone,
        mobile,

        -- locale
        time_zone,
        lang_code,
        country_code,

        -- misc
        about_info,
        extn,
        photo_url,
        is_confirmed,
        is_zia_agent,
        zuid,

        -- dlt internal key — kept intentionally for joining to stg_zoho_desk__agent_departments
        -- jointure : stg_zoho_desk__agent_departments._dlt_parent_id = _dlt_id
        _dlt_id

    from source
)

select * from renamed
    );
  