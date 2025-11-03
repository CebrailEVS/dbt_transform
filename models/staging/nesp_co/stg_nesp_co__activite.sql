{{ 
  config(
    materialized='table',
    description='Table des activités Nespresso Commercial nettoyée et filtrée sur les colonnes utiles.',
  ) 
}}

with source_data as (
    select * 
    from {{ source('nesp_co', 'nespresso_commerce_activite') }}
),

base_activite as (
    select
        -- IDs convertis en BIGINT
        cast(nullif(activity_id, '#') as int64) as activity_id,

        -- ID STRING
        nullif(unnamed_1, '#') as c4c_id_commercial,
        cast(nullif(unnamed_12, '#') as int64) as c4c_id_main_account,
        nullif(nessoft_id_main_account_, '#') as nessoft_id_main_account,
        cast(nullif(unnamed_22, '#') as int64) as c4c_id_campaign,
        nullif(campaign_id_preceding_lead_, '#') as campaign_id_preceding_lead,

        -- Colonnes texte
        nullif(employee_responsible, '#') as employee_responsible,
        nullif(activity_type, '#') as activity_type,
        nullif(phone_call, '#') as phone_call,
        nullif(created_by_phone_call_, '#') as created_by_phone_call,
        nullif(appointment, '#') as appointment,
        nullif(created_by_appointment_, '#') as created_by_appointment,
        nullif(activity_category, '#') as activity_category,
        nullif(main_account, '#') as main_account,
        nullif(role, '#') as role,
        nullif(activity_unit, '#') as activity_unit,
        nullif(activity_life_cycle_status, '#') as activity_life_cycle_status,
        nullif(notes, '#') as notes,

        -- Dates harmonisées (converties en TIMESTAMP)
        timestamp(nullif(start_date_phone_call_, '#')) as start_date_phone_call,
        timestamp(nullif(start_date_appointment_, '#')) as start_date_appointment,
        timestamp(nullif(start_date_task_, '#')) as start_date_task,
        nullif(calendar_month, '#') as calendar_month,

        -- Metadata
        timestamp(extracted_at) as extracted_at,
        timestamp(file_date) as file_date,
        source_file

    from source_data
)

select *
from base_activite
