{{ config(materialized='table') }}

with device_labels as (
    select
        d.iddevice as device_id,
        d.device_iddevice,
        d.iddevice_type as device_type_id,
        d.code as device_code,
        d.name as device_name,
        d.serial as device_serial_number,
        d.comments as device_comment,
        d.last_installation_date,
        d.purchase_date,
        d.purchase_cost,
        d.created_at,
        d.updated_at,
        d.idlocation as location_id,
        d.idcompany_customer as company_id,
        c.code as company_code,
        c.name as company_name,
        d.idcompany_owner as owner_company_id,
        o.name as owner_company_name,
        lo.access_info,
        l.code as label_code,
        lf.code as label_family_code
    from {{ ref('stg_oracle_neshu__device') }} as d
    left join {{ ref('stg_oracle_neshu__label_has_device') }} as lhd
        on d.iddevice = lhd.iddevice and lhd.idlabel is not null
    left join {{ ref('stg_oracle_neshu__label') }} as l
        on lhd.idlabel = l.idlabel
    left join {{ ref('stg_oracle_neshu__label_family') }} as lf
        on l.idlabel_family = lf.idlabel_family
    left join {{ ref('stg_oracle_neshu__company') }} as c
        on d.idcompany_customer = c.idcompany
    left join {{ ref('stg_oracle_neshu__company') }} as o
        on d.idcompany_owner = o.idcompany
    left join {{ ref('stg_oracle_neshu__location') }} as lo
        on d.idlocation = lo.idlocation
    where
        d.idcompany_customer is not null
        and d.code_status_record <> -1  -- exclude ERP ghost-deletes
),

aggregated_labels as (
    select
        device_id,
        device_type_id,
        device_iddevice,
        company_id,
        owner_company_id,
        location_id,
        device_code,
        device_name,
        device_serial_number,
        device_comment,
        company_code,
        company_name,
        owner_company_name,
        access_info,
        last_installation_date,
        purchase_date,
        purchase_cost,
        created_at,
        updated_at,
        max(case when label_family_code = 'ETAT_MACHINE' then label_code end) as device_state,
        max(case when label_family_code = 'STATUT_MATERIEL' then label_code end) as device_material_status,
        max(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        -- Même famille et même code que l'ERP Nayka : la famille doit être créée sous ce code dans
        -- Distrilog Neshu pour être lue ici. Tant qu'elle n'existe pas, toutes les machines sont à FALSE.
        max(case when label_family_code = 'EQUIPEMENT_RESERVE' then label_code end) as is_reserved,
        max(case when label_family_code = 'GAMME' then label_code end) as device_gamme,
        max(case when label_family_code = 'CATEGORIE' then label_code end) as device_category,
        max(case when label_family_code = 'MARQUE' then label_code end) as device_brand,
        max(case when label_family_code = 'MODECOMA' then label_code end) as device_economic_model
    from device_labels
    group by
        device_id,
        device_type_id,
        device_iddevice,
        company_id,
        owner_company_id,
        location_id,
        device_code,
        device_name,
        device_serial_number,
        device_comment,
        company_code,
        company_name,
        owner_company_name,
        access_info,
        last_installation_date,
        purchase_date,
        purchase_cost,
        created_at,
        updated_at
)

select
    -- 🔑 Identifiants
    device_id,
    device_iddevice,
    device_type_id,
    company_id,
    owner_company_id,
    location_id,

    -- 📇 Codes et noms
    device_code,
    device_name,
    concat(device_code, ' - ', device_name) as device_label,
    device_serial_number,
    company_code,
    company_name,
    owner_company_name,

    -- 🏷️ Caractéristiques machine
    device_brand,
    device_gamme,
    device_category,
    device_economic_model,
    device_state,
    device_material_status,

    -- 📍 Localisation
    access_info as device_location,

    -- 📝 Commentaire libre saisi dans l'ERP
    device_comment,

    -- 🏷️ État et statu
    coalesce(lower(is_active) = 'yes', false) as is_active,
    coalesce(upper(is_reserved) = 'OUI', false) as is_reserved,

    -- 💶 Achat
    purchase_date,
    purchase_cost,

    -- 🕒 Dates
    {{ utc_vers_paris_affichage('last_installation_date') }} as last_installation_date,
    {{ utc_vers_paris_affichage('created_at') }} as created_at,
    {{ utc_vers_paris_affichage('updated_at') }} as updated_at
from aggregated_labels
