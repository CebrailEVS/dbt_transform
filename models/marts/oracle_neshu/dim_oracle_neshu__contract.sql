{{
  config(
    materialized='table',
    cluster_by=['idcontract'],
    description='Dimension contrat : pivot des labels et sélection du contrat actif principal par client.'
  )
}}

WITH contract_labels AS (
  SELECT 
    c.idcontract,
    c.idcompany_peer as idcompany,
    c.code AS contract_code,
    c.engagement_raw,
    c.engagement_clean,
    c.nombre_collab,
    l.code AS label_code,
    lf.code AS label_family_code,
    c.original_start_date,
    c.original_end_date,
    c.current_end_date,
    c.termination_date,
    c.created_at,
    c.updated_at
  FROM {{ ref('stg_oracle_neshu__contract') }} c
  LEFT JOIN {{ ref('stg_oracle_neshu__label_has_contract') }} lhc 
    ON lhc.idcontract = c.idcontract AND lhc.idlabel IS NOT NULL
  LEFT JOIN {{ ref('stg_oracle_neshu__label') }} l 
    ON l.idlabel = lhc.idlabel
  LEFT JOIN {{ ref('stg_oracle_neshu__label_family') }} lf 
    ON lf.idlabel_family = l.idlabel_family
),

aggregated_labels AS ( 
  SELECT
    idcontract,
    idcompany,
    contract_code,
    engagement_raw,
    engagement_clean,
    nombre_collab,
    original_start_date,
    original_end_date,
    current_end_date,
    termination_date,
    created_at,
    updated_at,

    -- pivot des familles de labels
    MAX(CASE WHEN label_family_code = 'TRANCHE_COLLAB' THEN label_code END) AS employee_range,
    MAX(CASE WHEN label_family_code = 'PROADMAN' THEN label_code END) AS proadman,
    MAX(CASE WHEN label_family_code = 'REGION' THEN label_code END) AS region,
    MAX(CASE WHEN label_family_code = 'TELETRAVAIL' THEN label_code END) AS teletravail,
    MAX(CASE WHEN label_family_code = 'ISACTIVE' THEN label_code END) AS is_active

  FROM contract_labels
  GROUP BY
    idcontract,
    idcompany,
    contract_code,
    engagement_raw,
    engagement_clean,
    nombre_collab,
    original_start_date,
    original_end_date,
    current_end_date,
    termination_date,
    created_at,
    updated_at
),

aggreated_contract as (
  SELECT
    idcontract,
    idcompany,
    contract_code,
    engagement_raw,
    engagement_clean,
    nombre_collab,
    CASE
      WHEN LOWER(is_active) = 'yes' THEN TRUE
      ELSE FALSE
    END AS is_active,
    original_start_date,
    original_end_date,
    current_end_date,
    termination_date,
    created_at,
    updated_at
  FROM aggregated_labels
)

SELECT
  idcontract,
  idcompany,
  contract_code,
  engagement_raw,
  engagement_clean,
  nombre_collab,
  is_active,
  original_start_date,
  original_end_date,
  current_end_date,
  termination_date,
  created_at,
  updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY idcompany
               ORDER BY current_end_date DESC, original_start_date DESC, idcontract
           ) as rn
    FROM aggreated_contract
    WHERE is_active = TRUE
) subq
WHERE rn = 1
