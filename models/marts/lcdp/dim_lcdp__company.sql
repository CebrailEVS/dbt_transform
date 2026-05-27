{{ config(materialized='table') }}

with company_labels as (
    select
        c.idcompany as company_id,
        c.company_idcompany as parent_company_id,
        c.code as company_code,
        c.idcompany_type as company_type_id,
        c.name as company_name,
        c.created_at,
        c.updated_at,
        lc.label_code,
        lc.label_text_fr,
        lc.label_family_code,
        loc.address1,
        loc.address2,
        loc.city,
        loc.postal as postal_code,
        loc.country
    from {{ ref('stg_oracle_lcdp__company') }} as c
    left join {{ ref('stg_oracle_lcdp__label_company') }} as lc
        on c.idcompany = lc.company_id
    left join {{ ref('stg_oracle_lcdp__company_has_location') }} as chl
        on c.idcompany = chl.idcompany and chl.idlocation_type = 1
    left join {{ ref('stg_oracle_lcdp__location') }} as loc
        on chl.idlocation = loc.idlocation
    where c.idcompany_type in (1, 2, 4, 6)
),

aggregated_labels as (
    select
        company_id,
        parent_company_id,
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
        MAX(case when label_family_code = 'BL_GRP' then label_text_fr end) as bl_group,
        MAX(case when label_family_code = 'BUSMOD' then label_text_fr end) as business_model,
        MAX(case when label_family_code = 'DOMACT' then label_text_fr end) as activity_domain,
        MAX(case when label_family_code = 'GC' then label_text_fr end) as key_account,
        MAX(case when label_family_code = 'Gestion reliquat' then label_text_fr end) as remainder_management,
        -- is_active conservé sur le code (YES/NO) pour le test lower(...) = 'yes' ci-dessous
        MAX(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        MAX(case when label_family_code = 'MEF' then label_text_fr end) as invoice_delivery_mode,
        MAX(case when label_family_code = 'MODEH' then label_text_fr end) as model_horeca,
        MAX(case when label_family_code = 'MODEOF' then label_text_fr end) as model_office,
        MAX(case when label_family_code = 'MODER' then label_text_fr end) as model_revendeur,
        MAX(case when label_family_code = 'PROPRIO' then label_text_fr end) as owner,
        MAX(case when label_family_code = 'REPRES' then label_text_fr end) as representative,
        MAX(case when label_family_code = 'ZGEO' then label_text_fr end) as geo_zone
    from company_labels
    group by
        company_id,
        parent_company_id,
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
    -- Identifiants
    company_id,
    parent_company_id,
    company_type_id,

    -- Codes et noms
    company_code,
    company_name,

    -- Caractéristiques entreprise
    geo_zone,
    activity_domain,
    business_model,
    key_account,
    COALESCE(LOWER(is_active) = 'yes', false) as is_active,

    -- Modèles métier
    model_horeca,
    model_office,
    model_revendeur,
    owner,
    representative,

    -- Gestion commerciale
    bl_group,
    invoice_delivery_mode,
    remainder_management,

    -- Adresse
    address1,
    address2,
    city,
    postal_code,
    country,

    -- Dates
    created_at,
    updated_at

from aggregated_labels
