{{
  config(
    materialized='table',
    cluster_by=['iddevice'],
    description='Dimension device enrichie à partir des labels associés (type, famille, groupe, marque, etc.) filtré sur les devices de type 1 (MACHINE) et device_iddevice NULL (sans équipement parent)'
  )
}}

WITH machine_labels AS (
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
  WHERE d.iddevice_type = 1
    AND d.device_iddevice IS NULL
),

pivoted_labels AS (
  SELECT *
  FROM machine_labels
  PIVOT (
    MAX(label_code) FOR label_family_code IN (
      'ETAT_MACHINE' AS machine_state,
      'STATUT_MATERIEL' AS material_status,
      'ISACTIVE' AS is_active, 
      'GAMME' AS machine_gamme,
      'CATEGORIE' AS machine_category,
      'MARQUE' AS brand,
      'MODECOMA' AS modele_economique
    )
  )
)

SELECT
  iddevice,
  device_code,
  device_name,
  last_installation_date,
  idcompany_customer,
  company_code,
  idlocation,
  access_info,
  created_at,
  updated_at,
  machine_state,
  material_status,
  is_active,
  machine_gamme,
  machine_category,
  brand,
  modele_economique
FROM pivoted_labels
