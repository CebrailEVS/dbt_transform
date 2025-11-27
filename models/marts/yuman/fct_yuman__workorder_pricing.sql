{{ config(
    materialized = "table",
    schema='marts',
    alias = "fct_yuman__workorder_pricing",
    partition_by={"field": "date_done", "data_type": "timestamp"},
    cluster_by=['billing_validation_status','workorder_status','demand_status','partner_name']
) }}

-- ============================================================================
-- MODEL: fct_yuman__workorder_pricing
-- PURPOSE: Determine automatic pricing for technical interventions from Yuman
-- AUTHOR: Cebrail AKSOY
-- UPDATED: {{ run_started_at }}
-- ============================================================================

WITH 
-- ============================================================================
-- 1. BASE DATA EXTRACTION
-- ============================================================================
base_workorders AS (
  SELECT
    demand_id,
    workorder_id,
    material_id,
    site_id,
    client_id,
    technician_id,
    manager_id,
    demand_description,
    demand_status,
    demand_created_at,
    demand_updated_at,
    demand_category_name,
    workorder_number,
    workorder_category,
    workorder_status,
    workorder_technician_name,
    workorder_date_creation,
    workorder_motif_non_intervention,
    workorder_detail_non_intervention,
    workorder_raison_mise_en_pause,
    workorder_explication_mise_en_pause,
    date_planned,
    date_started,
    date_done,
    partner_name,
    client_code,
    client_name,
    client_category,
    client_is_active,
    site_code,
    site_name,
    site_address,
    site_postal_code,
    material_serial_number,
    technician_equipe,

    -- Normalize workorder type
    LOWER(COALESCE(
      workorder_category,
      CASE
        WHEN workorder_type = 'Reactive' THEN 'curatif'
        WHEN workorder_type = 'Preventive' THEN 'préventif'
        WHEN workorder_type = 'Installation' THEN 'installation'
        ELSE workorder_type
      END
    )) AS workorder_type_raw,

    -- Normalize machine category
    LOWER(TRIM(CASE
      WHEN material_category IS NOT NULL THEN material_category
      WHEN LOWER(client_name) LIKE '%generique%' THEN CONCAT(partner_name, '_GENERIQUE')
      WHEN partner_name = 'AUUM' THEN 'MGZ'
      WHEN partner_name = 'TWYD' THEN 'FONTAINE TWYD'
      WHEN partner_name = 'EXPRESSO' THEN 'MILANO'
      WHEN partner_name = 'NESHU' THEN 'MILANO'
      WHEN partner_name = 'BRITA' THEN 'viv t 85 c2-tg-i-cu ce'
      WHEN partner_name = 'DAAN' THEN 'lave-vaisselle'
      WHEN partner_name = 'NU' THEN 'frigo nu'
      ELSE NULL  
    END)) AS machine_raw,

    -- Postal code correction
    CASE
      WHEN site_postal_code IS NULL OR site_postal_code = '00000' THEN (
        SELECT code_postal
        FROM UNNEST(REGEXP_EXTRACT_ALL(demand_description, r'\b\d{5}\b')) AS code_postal
        WHERE NOT REGEXP_CONTAINS(
          LEFT(demand_description, STRPOS(demand_description, code_postal)-1),
          r'(?i)N°\s*$|interventions?\s*$'
        )
        LIMIT 1
      )
      ELSE REGEXP_EXTRACT(site_address, r'\b(\d{5})\b')
    END AS postal_code_site,

    -- Billing indicator
    CASE
      WHEN workorder_status = 'Closed'
        AND workorder_motif_non_intervention IS NULL
        AND workorder_detail_non_intervention IS NULL
      THEN TRUE
      ELSE FALSE
    END AS a_facturer

  FROM {{ ref('int_yuman__demands_workorders_enriched') }}
),

-- ============================================================================
-- 2. REFERENCE TABLES
-- ============================================================================
ref_type_inter AS (
  SELECT 
    LOWER(Type_intervention_Brut) AS workorder_type_raw,
    LOWER(TYPE_INTER) AS workorder_type_clean
  FROM {{ ref('ref_yuman__type_inter_clean') }}
),

ref_machine AS (
  SELECT machine_raw, machine_clean
  FROM (
    SELECT 
      LOWER(TRIM(Machine_Brut)) AS machine_raw,
      LOWER(TRIM(MACHINE)) AS machine_clean,
      ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(Machine_Brut)) ORDER BY MACHINE) AS rn
    FROM {{ ref('ref_yuman__machine_clean') }}
  )
  WHERE rn = 1
),

ref_cp_metropole AS (
  SELECT Code_Postal, Metropole
  FROM {{ ref('ref_yuman__cp_metropole') }}
),

ref_dpt_metropole AS (
  SELECT Departement, Metropole
  FROM {{ ref('ref_yuman__dpt_metropole') }}
),

ref_tarification AS (
  SELECT
    LOWER(CONCAT(
      TYPE_INTER, '_',
      MACHINE, '_',
      MARQUE, '_',
      Type_tarif, '_',
      CAST(METROPOLE AS STRING)
    )) AS key_tarif,
    Montant,
    PROD
  FROM {{ ref('ref_yuman__tarification_clean') }}
),

-- ============================================================================
-- 3. ENRICHMENT & NORMALIZATION
-- ============================================================================
workorders_enriched AS (
  SELECT
    w.*,
    COALESCE(ti.workorder_type_clean, w.workorder_type_raw) AS workorder_type_clean,
    COALESCE(m.machine_clean, w.machine_raw) AS machine_clean,
    COALESCE(cp.Metropole, dp.Metropole) AS metropole_city,
    CASE
      WHEN w.postal_code_site IS NULL THEN 1
      WHEN cp.Code_Postal IS NOT NULL THEN 1
      WHEN dp.Departement IS NOT NULL THEN 1
      ELSE 0
    END AS metropole,
    ROW_NUMBER() OVER(PARTITION BY w.workorder_id ORDER BY w.date_done) AS rn
  FROM base_workorders w
  LEFT JOIN ref_type_inter ti ON w.workorder_type_raw = ti.workorder_type_raw
  LEFT JOIN ref_machine m ON w.machine_raw = m.machine_raw
  LEFT JOIN ref_cp_metropole cp ON w.postal_code_site = cp.Code_Postal
  LEFT JOIN ref_dpt_metropole dp ON LEFT(w.postal_code_site, 2) = dp.Departement
),

-- ============================================================================
-- 4. DEDUPLICATION & RECURRENCE CALCULATION
-- ============================================================================
workorders_dedup AS (
  SELECT
    *,
    CASE
      WHEN postal_code_site IS NULL THEN 1
      ELSE COUNT(
        CASE WHEN a_facturer THEN site_id END
      ) OVER (PARTITION BY site_id, DATE(date_done))
    END AS reccurence
  FROM workorders_enriched
),

-- ============================================================================
-- 5. PRICING TYPE DETERMINATION
-- ============================================================================
workorders_with_tarif AS (
  SELECT
    *,
    CASE
      WHEN postal_code_site IS NULL THEN 'Tarif normal'
      WHEN partner_name IN ('AUUM', 'FONTAINCO', 'TWYD', 'NESHU', 'NU', 'DAANTECH', 'EXPRESSO', 'DAAN') THEN
        CASE
          WHEN reccurence < 5 THEN 'Tarif normal'
          WHEN reccurence BETWEEN 5 AND 20 THEN 'Remise niv1'
          ELSE 'Remise niv2'
        END
      WHEN partner_name IN ('BRITA', 'FONTAINCO') THEN
        CASE
          WHEN reccurence < 2 THEN 'Tarif normal'
          WHEN reccurence BETWEEN 2 AND 5 THEN 'Remise niv1'
          ELSE 'Remise niv2'
        END
      ELSE 'Tarif normal'
    END AS type_tarif
  FROM workorders_dedup
),

-- ============================================================================
-- 6. FINAL JOIN WITH TARIFF TABLE
-- ============================================================================
final_result AS (
  SELECT
    w.*,
    t.Montant,
    t.PROD,
    LOWER(CONCAT(
      w.workorder_type_clean, '_',
      w.machine_clean, '_',
      w.partner_name, '_',
      w.type_tarif, '_',
      CAST(w.metropole AS STRING)
    )) AS key_tarif_used
  FROM workorders_with_tarif w
  LEFT JOIN ref_tarification t
    ON LOWER(CONCAT(
      w.workorder_type_clean, '_',
      w.machine_clean, '_',
      w.partner_name, '_',
      w.type_tarif, '_',
      CAST(w.metropole AS STRING)
    )) = t.key_tarif
)

-- ============================================================================
-- 7. FINAL SELECT (CLEAN OUTPUT)
-- ============================================================================
SELECT
  demand_id,
  workorder_id,
  material_id,
  site_id,
  client_id,
  technician_id,
  manager_id,
  demand_description,
  demand_status,
  demand_created_at,
  demand_updated_at,
  demand_category_name,
  workorder_number,
  workorder_category,
  workorder_status,
  workorder_technician_name,
  workorder_date_creation,
  workorder_motif_non_intervention,
  workorder_detail_non_intervention,
  workorder_raison_mise_en_pause,
  workorder_explication_mise_en_pause,
  date_planned,
  date_started,
  date_done,
  partner_name,
  client_code,
  client_name,
  client_category,
  client_is_active,
  site_code,
  site_name,
  site_address,
  postal_code_site as site_postal_code,
  material_serial_number,
  workorder_type_raw,
  machine_raw,

  -- Renamed metrics and keys
  workorder_type_clean,
  machine_clean,
  metropole             AS metropolitan,
  metropole_city,
  technician_equipe,
  reccurence            AS recurrence_count,
  type_tarif            AS pricing_type,
  key_tarif_used        AS pricing_key_used,
  a_facturer            AS to_invoice,
  Montant               AS amount,
  PROD                  AS prod_number,
  CASE
    WHEN Montant IS NOT NULL AND a_facturer = TRUE THEN 'VALIDATED'
    WHEN Montant IS NULL AND a_facturer = TRUE THEN 'MISSING_TARIF'
    ELSE 'NOT_BILLABLE'
  END as billing_validation_status

FROM final_result
