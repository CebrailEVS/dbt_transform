
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dimension machine LCDP enrichie des labels m\u00e9tier (cat\u00e9gorie, \u00e9tat, types fontaine/broyeur/percolateur/SP).\n[COMMENT CONSTRUITE] Issu de stg_oracle_lcdp__device joint \u00e0 stg_oracle_lcdp__company (company_code, company_name) et stg_oracle_lcdp__location (access_info \u2192 device_location). Pivot des labels via stg_oracle_lcdp__label_device (vue aplatie lcdp_v_label_device) sur les familles : CATMACH, MARQUE, ETAT_MACHINE, STATUT_MATERIEL, TYPEAUDIT, TYDA, LCDPMON, TYPFONT, TYPBROY, TYPPERCO, TYPSP, TYPDASA, MODSP, MARQSP, BADGE, ISACTIVE.\n[GRAIN] 1 ligne par device_id.\n[NOTES] device_iddevice = parent device (hi\u00e9rarchie). Les attributs label exposent d\u00e9sormais le libell\u00e9 FR (label_text_fr, ex. \"DA CHAUD\") au lieu du code (ex. \"CATMACH01\"). Exception : is_active reste bas\u00e9 sur le code (YES/NO) pour la conversion bool\u00e9enne. is_active converti en bool\u00e9en.\n"""
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
        ld.label_code,
        ld.label_text_fr,
        ld.label_family_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as d
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_device` as ld
        on d.iddevice = ld.device_id
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as c
        on d.idcompany_customer = c.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location` as lo
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
        MAX(case when label_family_code = 'CATMACH' then label_text_fr end) as device_category,
        MAX(case when label_family_code = 'STATUT_MATERIEL' then label_text_fr end) as device_material_status,
        MAX(case when label_family_code = 'TYPEAUDIT' then label_text_fr end) as audit_type,
        MAX(case when label_family_code = 'TYPFONT' then label_text_fr end) as fountain_type,
        MAX(case when label_family_code = 'TYPSP' then label_text_fr end) as type_sp,
        MAX(case when label_family_code = 'TYPBROY' then label_text_fr end) as grinder_type,
        MAX(case when label_family_code = 'ETAT_MACHINE' then label_text_fr end) as device_state,
        MAX(case when label_family_code = 'TYDA' then label_text_fr end) as typology_da,
        MAX(case when label_family_code = 'BADGE' then label_text_fr end) as badge,
        MAX(case when label_family_code = 'MARQUE' then label_text_fr end) as device_brand,
        MAX(case when label_family_code = 'MODSP' then label_text_fr end) as model_sp,
        -- is_active conservé sur le code (YES/NO) pour le test lower(...) = 'yes' ci-dessous
        MAX(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        MAX(case when label_family_code = 'TYPDASA' then label_text_fr end) as type_dasa,
        MAX(case when label_family_code = 'MARQSP' then label_text_fr end) as brand_sp,
        MAX(case when label_family_code = 'TYPPERCO' then label_text_fr end) as percolator_type,
        MAX(case when label_family_code = 'LCDPMON' then label_text_fr end) as currency_mode
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
    -- Identifiants
    device_id,
    device_iddevice,
    device_type_id,
    company_id,
    location_id,

    -- Codes et noms
    device_code,
    device_name,
    company_code,
    company_name,

    -- Caractéristiques machine
    device_category,
    device_brand,
    device_state,
    device_material_status,
    audit_type,
    typology_da,
    currency_mode,

    -- Types machine
    fountain_type,
    grinder_type,
    percolator_type,
    type_sp,
    type_dasa,
    model_sp,
    brand_sp,
    badge,

    -- Localisation
    access_info as device_location,

    -- Statut
    COALESCE(LOWER(is_active) = 'yes', false) as is_active,

    -- Dates
    last_installation_date,
    created_at,
    updated_at

from aggregated_labels
    );
  