
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device`
      
    
    

    
    OPTIONS(
      description="""Dimension device enrichie \u00e0 partir des labels associ\u00e9s (\u00e9tat, statut, gamme, cat\u00e9gorie, marque, etc.), filtr\u00e9e sur les machines (type 1).\n"""
    )
    as (
      

with device_labels as (
    select
        d.iddevice as device_id,
        d.device_iddevice,
        d.iddevice_type as device_type_id,
        d.code as device_code,
        d.name as device_name,
        d.last_installation_date,
        d.created_at,
        d.updated_at,
        d.idlocation as location_id,
        d.idcompany_customer as company_id,
        c.code as company_code,
        c.name as company_name,
        lo.access_info,
        l.code as label_code,
        lf.code as label_family_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_device` as lhd
        on d.iddevice = lhd.iddevice and lhd.idlabel is not null
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhd.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on d.idcompany_customer = c.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location` as lo
        on d.idlocation = lo.idlocation
    where d.idcompany_customer is not null
),

aggregated_labels as (
    select
        device_id,
        device_type_id,
        device_iddevice,
        company_id,
        location_id,
        device_code,
        device_name,
        company_code,
        company_name,
        access_info,
        last_installation_date,
        created_at,
        updated_at,
        MAX(case when label_family_code = 'ETAT_MACHINE' then label_code end) as device_state,
        MAX(case when label_family_code = 'STATUT_MATERIEL' then label_code end) as device_material_status,
        MAX(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        MAX(case when label_family_code = 'GAMME' then label_code end) as device_gamme,
        MAX(case when label_family_code = 'CATEGORIE' then label_code end) as device_category,
        MAX(case when label_family_code = 'MARQUE' then label_code end) as device_brand,
        MAX(case when label_family_code = 'MODECOMA' then label_code end) as device_economic_model
    from device_labels
    group by
        device_id,
        device_type_id,
        device_iddevice,
        company_id,
        location_id,
        device_code,
        device_name,
        company_code,
        company_name,
        access_info,
        last_installation_date,
        created_at,
        updated_at
)

select
    -- 🔑 Identifiants
    device_id,
    device_iddevice,
    device_type_id,
    company_id,
    location_id,

    -- 📇 Codes et noms
    device_code,
    device_name,
    company_code,
    company_name,

    -- 🏷️ Caractéristiques machine
    device_brand,
    device_gamme,
    device_category,
    device_economic_model,

    -- 📍 Localisation
    access_info as device_location,

    -- 🏷️ État et statu
    COALESCE(LOWER(is_active) = 'yes', false) as is_active,

    -- 🕒 Dates
    last_installation_date,
    created_at,
    updated_at
from aggregated_labels
    );
  