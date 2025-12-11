
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company`
      
    
    

    
    OPTIONS(
      description="""Dimension client/company enrichie \u00e0 partir des labels associ\u00e9s (r\u00e9gion, secteur, statut, etc.) et des informations de localisation.\n"""
    )
    as (
      

WITH company_labels AS (
  SELECT 
    c.idcompany as company_id,
    c.code AS company_code,
    c.idcompany_type as company_type_id,
    c.name AS company_name,
    c.created_at,
    c.updated_at,
    l.code AS label_code,
    lf.code AS label_family_code,
    loc.address1,
    loc.address2,
    loc.city,
    loc.postal AS postal_code,
    loc.country
  FROM `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` c
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_company` lhc 
    ON lhc.idcompany = c.idcompany AND lhc.idlabel IS NOT NULL
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` l 
    ON l.idlabel = lhc.idlabel
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` lf 
    ON lf.idlabel_family = l.idlabel_family
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company_has_location` chl
    ON chl.idcompany = c.idcompany AND chl.idlocation_type = 1
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__location` loc
    ON loc.idlocation = chl.idlocation
  WHERE ((c.idcompany_type IN (1,2,4,6))
)),
aggregated_labels AS (
  SELECT
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
    MAX(CASE WHEN label_family_code = 'TRANCHE_COLLAB' THEN label_code END) AS employee_range,
    MAX(CASE WHEN label_family_code = 'PROADMAN' THEN label_code END) AS proadman,
    MAX(CASE WHEN label_family_code = 'REGION' THEN label_code END) AS region,
    MAX(CASE WHEN label_family_code = 'TELETRAVAIL' THEN label_code END) AS remote_work,
    MAX(CASE WHEN label_family_code = 'SECTEUR_ACTVITE' THEN label_code END) AS sector,
    MAX(CASE WHEN label_family_code = 'SECTEUR_DACTIVITE' THEN label_code END) AS activity_sector,
    MAX(CASE WHEN label_family_code = 'RCOMM' THEN label_code END) AS commercial_rep,
    MAX(CASE WHEN label_family_code = 'HORECA' THEN label_code END) AS horeca,
    MAX(CASE WHEN label_family_code = 'GSM_TERRAIN' THEN label_code END) AS gsm,
    MAX(CASE WHEN label_family_code = 'KATIERS' THEN label_code END) AS katiers,
    MAX(CASE WHEN label_family_code = 'ISACTIVE' THEN label_code END) AS is_active,
    MAX(CASE WHEN label_family_code = 'CODE_SECTEUR' THEN label_code END) AS sector_code,
    MAX(CASE WHEN label_family_code = 'STATUT_CLIENT' THEN label_code END) AS client_status,
    MAX(CASE WHEN label_family_code = 'Gestion reliquat' THEN label_code END) AS remainder_management,
    MAX(CASE WHEN label_family_code = 'MODEENVOIFACTURE' THEN label_code END) AS invoice_delivery_mode,
    MAX(CASE WHEN label_family_code = 'BADGE' THEN label_code END) AS badge,
    MAX(CASE WHEN label_family_code = 'RECYCLAGE' THEN label_code END) AS recycling,
    MAX(CASE WHEN label_family_code = 'TYPECOMPAGNIE' THEN label_code END) AS company_type,
    MAX(CASE WHEN label_family_code = 'MODELEECOCLIENT' THEN label_code END) AS company_economic_model,
    MAX(CASE WHEN label_family_code = 'BL_GRP' THEN label_code END) AS bl_group,
    MAX(CASE WHEN label_family_code = 'KA' THEN label_code END) AS key_account
  FROM company_labels
  GROUP BY
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

SELECT
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
  
  CASE
    WHEN LOWER(is_active) = 'yes' THEN TRUE
    ELSE FALSE
  END AS is_active,

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

FROM aggregated_labels
    );
  