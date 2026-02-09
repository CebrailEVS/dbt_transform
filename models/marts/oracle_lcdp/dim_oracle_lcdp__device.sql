{{
  config(
    materialized='table',
    description='Dimension device enrichie à partir des labels associés (catégorie, état, statut, marque, types, etc.)'
  )
}}

WITH device_labels AS (
  SELECT
    d.iddevice as device_id,
    d.device_iddevice,
    d.iddevice_type as device_type_id,
    d.code AS device_code,
    d.name AS device_name,
    d.last_installation_date,
    d.created_at,
    d.updated_at,
    d.idlocation as location_id,
    d.idcompany_customer as company_id,
    c.code AS company_code,
    c.name AS company_name,
    lo.access_info,
    l.code AS label_code,
    lf.code AS label_family_code
  FROM {{ ref('stg_oracle_lcdp__device') }} d
  LEFT JOIN {{ ref('stg_oracle_lcdp__label_has_device') }} lhd
    ON lhd.iddevice = d.iddevice AND lhd.idlabel IS NOT NULL
  LEFT JOIN {{ ref('stg_oracle_lcdp__label') }} l
    ON l.idlabel = lhd.idlabel
  LEFT JOIN {{ ref('stg_oracle_lcdp__label_family') }} lf
    ON lf.idlabel_family = l.idlabel_family
  LEFT JOIN {{ ref('stg_oracle_lcdp__company') }} c
    ON c.idcompany = d.idcompany_customer
  LEFT JOIN {{ ref('stg_oracle_lcdp__location') }} lo
    ON lo.idlocation = d.idlocation
  WHERE d.idcompany_customer IS NOT NULL
),

aggregated_labels AS (
  SELECT
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
    MAX(CASE WHEN label_family_code = 'CATMACH' THEN label_code END) AS device_category,
    MAX(CASE WHEN label_family_code = 'STATUT_MATERIEL' THEN label_code END) AS device_material_status,
    MAX(CASE WHEN label_family_code = 'TYPEAUDIT' THEN label_code END) AS audit_type,
    MAX(CASE WHEN label_family_code = 'TYPFONT' THEN label_code END) AS fountain_type,
    MAX(CASE WHEN label_family_code = 'TYPSP' THEN label_code END) AS type_sp,
    MAX(CASE WHEN label_family_code = 'TYPBROY' THEN label_code END) AS grinder_type,
    MAX(CASE WHEN label_family_code = 'ETAT_MACHINE' THEN label_code END) AS device_state,
    MAX(CASE WHEN label_family_code = 'TYDA' THEN label_code END) AS typology_da,
    MAX(CASE WHEN label_family_code = 'BADGE' THEN label_code END) AS badge,
    MAX(CASE WHEN label_family_code = 'MARQUE' THEN label_code END) AS device_brand,
    MAX(CASE WHEN label_family_code = 'MODSP' THEN label_code END) AS model_sp,
    MAX(CASE WHEN label_family_code = 'ISACTIVE' THEN label_code END) AS is_active,
    MAX(CASE WHEN label_family_code = 'TYPDASA' THEN label_code END) AS type_dasa,
    MAX(CASE WHEN label_family_code = 'MARQSP' THEN label_code END) AS brand_sp,
    MAX(CASE WHEN label_family_code = 'TYPPERCO' THEN label_code END) AS percolator_type,
    MAX(CASE WHEN label_family_code = 'LCDPMON' THEN label_code END) AS currency_mode
  FROM device_labels
  GROUP BY
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

SELECT
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
  CASE
    WHEN LOWER(is_active) = 'yes' THEN TRUE
    ELSE FALSE
  END AS is_active,

  -- Dates
  last_installation_date,
  created_at,
  updated_at

FROM aggregated_labels
