{{
  config(
    materialized='table',
    cluster_by=['iddevice'],
    description='Dimension device enrichie à partir des labels associés (état, statut, gamme, catégorie, marque, etc.), filtrée sur les machines (type 1) sans parent.'
  )
}}

WITH device_labels AS (
  SELECT 
    d.iddevice,
    d.device_iddevice,
    d.iddevice_type,
    d.code AS device_code,
    d.name AS device_name,
    d.last_installation_date,
    d.created_at,
    d.updated_at,
    d.idlocation,
    d.idcompany_customer,
    c.code AS company_code,
    lo.access_info,
    l.code AS label_code,
    lf.code AS label_family_code
  FROM {{ ref('stg_oracle_neshu__device') }} d
  LEFT JOIN {{ ref('stg_oracle_neshu__label_has_device') }} lhd 
    ON lhd.iddevice = d.iddevice AND lhd.idlabel IS NOT NULL
  LEFT JOIN {{ ref('stg_oracle_neshu__label') }} l 
    ON l.idlabel = lhd.idlabel
  LEFT JOIN {{ ref('stg_oracle_neshu__label_family') }} lf 
    ON lf.idlabel_family = l.idlabel_family
  LEFT JOIN {{ ref('stg_oracle_neshu__company') }} c 
    ON c.idcompany = d.idcompany_customer
  LEFT JOIN {{ ref('stg_oracle_neshu__location') }} lo
    ON lo.idlocation = d.idlocation
  WHERE d.idcompany_customer IS NOT NULL
),
aggregated_labels AS (
  SELECT
    iddevice,
    iddevice_type,
    device_iddevice,
    idcompany_customer,
    idlocation,
    device_code,
    device_name,
    company_code,
    access_info,
    last_installation_date,
    created_at,
    updated_at,
    MAX(CASE WHEN label_family_code = 'ETAT_MACHINE' THEN label_code END) AS machine_state,
    MAX(CASE WHEN label_family_code = 'STATUT_MATERIEL' THEN label_code END) AS material_status,
    MAX(CASE WHEN label_family_code = 'ISACTIVE' THEN label_code END) AS is_active,
    MAX(CASE WHEN label_family_code = 'GAMME' THEN label_code END) AS machine_gamme,
    MAX(CASE WHEN label_family_code = 'CATEGORIE' THEN label_code END) AS machine_category,
    MAX(CASE WHEN label_family_code = 'MARQUE' THEN label_code END) AS brand,
    MAX(CASE WHEN label_family_code = 'MODECOMA' THEN label_code END) AS modele_economique
  FROM device_labels
  GROUP BY
    iddevice,
    iddevice_type,
    device_iddevice,
    idcompany_customer,
    idlocation,
    device_code,
    device_name,
    company_code,
    access_info,
    last_installation_date,
    created_at,
    updated_at
)

SELECT
  -- 🔑 Identifiants
  iddevice,
  device_iddevice,
  iddevice_type,
  idcompany_customer,
  idlocation,

  -- 📇 Codes et noms
  device_code,
  device_name,
  company_code,

  -- 🏷️ Caractéristiques machine
  brand,
  machine_gamme,
  machine_category,
  modele_economique,
  CASE
    WHEN LOWER(is_active) = 'yes' THEN TRUE
    ELSE FALSE
  END AS is_active,

  -- 📍 Localisation
  access_info as device_location,

  -- 🕒 Dates
  last_installation_date,
  created_at,
  updated_at
FROM aggregated_labels
