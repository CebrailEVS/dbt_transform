
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__activite`
      
    partition by file_date
    

    
    OPTIONS(
      description="""Activit\u00e9s commerciales Nespresso (CRM C4C), typ\u00e9es depuis `nesp_co_activite`. Le raw empile un export complet par journ\u00e9e : le staging garde, pour chaque activit\u00e9, la version de sa derni\u00e8re journ\u00e9e (`file_date`). Cl\u00e9 : (`activity_id`, `activity_type`). C4C num\u00e9rote \u00e0 part les appels et les rendez-vous : un m\u00eame num\u00e9ro peut d\u00e9signer un appel et un rendez-vous sans rapport entre eux.\n"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`nesp_co_activite`

),

base_activite as (

    select
        -- ids
        cast(nullif(activity_id, '#') as int64) as activity_id,
        nullif(unnamed_1, '#') as c4c_id_commercial,
        cast(nullif(unnamed_12, '#') as int64) as c4c_id_main_account,
        nullif(nessoft_id_main_account, '#') as nessoft_id_main_account,
        cast(nullif(unnamed_22, '#') as int64) as c4c_id_campaign,
        nullif(campaign_id_preceding_lead, '#') as campaign_id_preceding_lead,

        -- texte
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

        -- dates
        timestamp(nullif(start_date_phone_call, '#')) as start_date_phone_call,
        timestamp(nullif(start_date_appointment, '#')) as start_date_appointment,
        timestamp(nullif(start_date_task, '#')) as start_date_task,
        nullif(calendar_month, '#') as calendar_month,

        -- metadata
        _extracted_at as extracted_at,
        snapshot_date as file_date,
        _fichier as source_file

    from source_data

),

-- Le raw empile un export complet par journée : on garde la version de la dernière journée.
-- Clé = (activity_id, activity_type) : C4C numérote à part les appels et les rendez-vous.
-- Tri sur la journée : un rattrapage charge plusieurs journées sous un même extracted_at.
deduped as (

    select *
    from base_activite
    qualify row_number() over (
        partition by activity_id, activity_type
        order by file_date desc, extracted_at desc
    ) = 1

)

select * from deduped
    );
  