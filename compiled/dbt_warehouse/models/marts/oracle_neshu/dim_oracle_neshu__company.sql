

with company_labels as (
    select
        c.idcompany as company_id,
        c.code as company_code,
        c.idcompany_type as company_type_id,
        c.name as company_name,
        c.created_at,
        c.updated_at,
        l.code as label_code,
        lf.code as label_family_code,
        loc.address1,
        loc.address2,
        loc.city,
        loc.postal as postal_code,
        loc.country
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_company` as lhc
        on c.idcompany = lhc.idcompany and lhc.idlabel is not null
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhc.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company_has_location` as chl
        on c.idcompany = chl.idcompany and chl.idlocation_type = 1
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location` as loc
        on chl.idlocation = loc.idlocation
    where
        ((c.idcompany_type in (1, 2, 4, 6))
        )
),

aggregated_labels as (
    select
        company_id,
        company_type_id,
        company_code,
        company_name,
        created_at,
        updated_at,
        address1,
        address2,
        city,
        postal_code,
        country,
        MAX(case when label_family_code = 'TRANCHE_COLLAB' then label_code end) as employee_range,
        MAX(case when label_family_code = 'PROADMAN' then label_code end) as proadman,
        MAX(case when label_family_code = 'REGION' then label_code end) as region,
        MAX(case when label_family_code = 'TELETRAVAIL' then label_code end) as remote_work,
        MAX(case when label_family_code = 'SECTEUR_ACTVITE' then label_code end) as sector,
        MAX(case when label_family_code = 'SECTEUR_DACTIVITE' then label_code end) as activity_sector,
        MAX(case when label_family_code = 'RCOMM' then label_code end) as commercial_rep,
        MAX(case when label_family_code = 'HORECA' then label_code end) as horeca,
        MAX(case when label_family_code = 'GSM_TERRAIN' then label_code end) as gsm,
        MAX(case when label_family_code = 'KATIERS' then label_code end) as katiers,
        MAX(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        MAX(case when label_family_code = 'CODE_SECTEUR' then label_code end) as sector_code,
        MAX(case when label_family_code = 'STATUT_CLIENT' then label_code end) as client_status,
        MAX(case when label_family_code = 'Gestion reliquat' then label_code end) as remainder_management,
        MAX(case when label_family_code = 'MODEENVOIFACTURE' then label_code end) as invoice_delivery_mode,
        MAX(case when label_family_code = 'BADGE' then label_code end) as badge,
        MAX(case when label_family_code = 'RECYCLAGE' then label_code end) as recycling,
        MAX(case when label_family_code = 'TYPECOMPAGNIE' then label_code end) as company_type,
        MAX(case when label_family_code = 'MODELEECOCLIENT' then label_code end) as company_economic_model,
        MAX(case when label_family_code = 'BL_GRP' then label_code end) as bl_group,
        MAX(case when label_family_code = 'KA' then label_code end) as key_account
    from company_labels
    group by
        company_id,
        company_type_id,
        company_code,
        company_name,
        created_at,
        updated_at,
        address1,
        address2,
        city,
        postal_code,
        country
)

select
    -- 🔑 Identifiants
    company_id,
    company_type_id,

    -- 📇 Codes et noms
    company_code,
    company_name,

    -- 🏢 Caractéristiques entreprise
    region,
    sector,
    sector_code,
    activity_sector,
    employee_range,
    company_type,
    company_economic_model,
    client_status,

    COALESCE(LOWER(is_active) = 'yes', false) as is_active,

    -- 👥 Gestion commerciale
    key_account,
    katiers,

    -- 🏨 Spécificités métier  
    remote_work,

    -- 🔧 Services et options
    proadman,
    gsm,
    badge,
    recycling,

    -- 📍 Adresse
    address1,
    address2,
    city,
    postal_code,
    country,

    -- 🕒 Dates
    created_at,
    updated_at

from aggregated_labels