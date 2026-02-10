
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__activite`
      
    
    

    
    OPTIONS(
      description="""Activite transform\u00e9s et nettoy\u00e9s depuis la source Nespresso Commerce Activite"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`nespresso_commerce_activite`

),

base_activite as (

    select
        -- ids convertis en bigint
        cast(nullif(activity_id, '#') as int64) as activity_id,

        -- id string
        nullif(unnamed_1, '#') as c4c_id_commercial,
        cast(nullif(unnamed_12, '#') as int64) as c4c_id_main_account,
        nullif(nessoft_id_main_account, '#') as nessoft_id_main_account,
        cast(nullif(unnamed_22, '#') as int64) as c4c_id_campaign,
        nullif(campaign_id_preceding_lead, '#') as campaign_id_preceding_lead,

        -- colonnes texte
        nullif(employee_responsible, '#') as employee_responsible,
        nullif(activity_type, '#') as activity_type,
        nullif(phone_call, '#') as phone_call,
        nullif(created_by_phone_call, '#') as created_by_phone_call,
        nullif(appointment, '#') as appointment,
        nullif(created_by_appointment, '#') as created_by_appointment,
        nullif(activity_category, '#') as activity_category,
        nullif(main_account, '#') as main_account,
        nullif(role, '#') as type_role,
        nullif(activity_unit, '#') as activity_unit,
        nullif(activity_life_cycle_status, '#') as activity_life_cycle_status,
        nullif(notes, '#') as notes,

        -- dates harmonisées (converties en timestamp)
        timestamp(nullif(start_date_phone_call, '#')) as start_date_phone_call,
        timestamp(nullif(start_date_appointment, '#')) as start_date_appointment,
        timestamp(nullif(start_date_task, '#')) as start_date_task,
        nullif(calendar_month, '#') as calendar_month,

        -- metadata
        timestamp(extracted_at) as extracted_at,
        timestamp(file_date) as file_date,
        source_file

    from source_data

)

select *
from base_activite
    );
  