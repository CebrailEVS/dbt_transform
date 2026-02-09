

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
  FROM `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` c
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_company` lhc
    ON lhc.idcompany = c.idcompany AND lhc.idlabel IS NOT NULL
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` l
    ON l.idlabel = lhc.idlabel
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` lf
    ON lf.idlabel_family = l.idlabel_family
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company_has_location` chl
    ON chl.idcompany = c.idcompany AND chl.idlocation_type = 1
  LEFT JOIN `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location` loc
    ON loc.idlocation = chl.idlocation
  WHERE c.idcompany_type IN (1, 2, 4, 6)
),

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
    MAX(CASE WHEN label_family_code = 'BL_GRP' THEN label_code END) AS bl_group,
    MAX(CASE WHEN label_family_code = 'BUSMOD' THEN label_code END) AS business_model,
    MAX(CASE WHEN label_family_code = 'DOMACT' THEN label_code END) AS activity_domain,
    MAX(CASE WHEN label_family_code = 'GC' THEN label_code END) AS key_account,
    MAX(CASE WHEN label_family_code = 'Gestion reliquat' THEN label_code END) AS remainder_management,
    MAX(CASE WHEN label_family_code = 'ISACTIVE' THEN label_code END) AS is_active,
    MAX(CASE WHEN label_family_code = 'MEF' THEN label_code END) AS invoice_delivery_mode,
    MAX(CASE WHEN label_family_code = 'MODEH' THEN label_code END) AS model_horeca,
    MAX(CASE WHEN label_family_code = 'MODEOF' THEN label_code END) AS model_office,
    MAX(CASE WHEN label_family_code = 'MODER' THEN label_code END) AS model_revendeur,
    MAX(CASE WHEN label_family_code = 'PROPRIO' THEN label_code END) AS owner,
    MAX(CASE WHEN label_family_code = 'REPRES' THEN label_code END) AS representative,
    MAX(CASE WHEN label_family_code = 'ZGEO' THEN label_code END) AS geo_zone
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
  -- Identifiants
  company_id,
  company_type_id,

  -- Codes et noms
  company_code,
  company_name,

  -- Caractéristiques entreprise
  geo_zone,
  activity_domain,
  business_model,
  key_account,

  CASE
    WHEN LOWER(is_active) = 'yes' THEN TRUE
    ELSE FALSE
  END AS is_active,

  -- Modèles métier
  model_horeca,
  model_office,
  model_revendeur,
  owner,

  -- Gestion commerciale
  representative,
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

FROM aggregated_labels