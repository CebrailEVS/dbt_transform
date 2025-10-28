
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_mssql_sage__pnl_bu`
      
    partition by timestamp_trunc(date_facturation, day)
    cluster by numero_compte_general, code_analytique

    
    OPTIONS(
      description="""Table de faits combinant \u00e9critures comptables et analytiques issues de SAGE pour la construction du P&L par Business Unit.\n"""
    )
    as (
      

WITH ecritures_comptables AS (
  SELECT
    ec_no AS numero_ecriture_comptable,
    cg_num AS numero_compte_general,
    ec_intitule AS libelle_ecriture,
    ec_sens as sens_ecriture,
    ec_date AS date_ecriture_comptable,
    jm_date AS date_periode_facturation,
    ec_jour AS jour_facturation,
    timestamp(DATE_ADD(jm_date, INTERVAL (ec_jour - 1) DAY)) AS date_facturation,
    created_at,
    updated_at
  FROM `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
  WHERE LEFT(CAST(cg_num AS STRING), 1) IN ('6', '7')
),

ecritures_analytiques AS (
  SELECT
    ec_no AS numero_ecriture_comptable,
    n_analytique AS numero_plan_analytique,
    ea_ligne AS numero_ligne_analytique,
    ca_num AS code_analytique,
    ea_montant AS montant_analytique,
    created_at,
    updated_at
  FROM `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
),

mapping_code_comptable__bu AS (
  SELECT * FROM `evs-datastack-prod`.`prod_reference`.`mapping_code_comptable__bu`
),

mapping_code_analytique__bu AS (
  SELECT * FROM `evs-datastack-prod`.`prod_reference`.`mapping_code_analytique__bu`
),

-- 🧩 Jointure principale + fallback BU analytique
mapped_with_fallback AS (
  SELECT
    c.numero_ecriture_comptable,
    a.numero_plan_analytique,
    a.numero_ligne_analytique,

    a.code_analytique,
    -- Fallback logique pour le code BU analytique
    CASE
      WHEN bu.code_analytique_bu IS NOT NULL THEN bu.code_analytique_bu
      WHEN a.code_analytique LIKE 'NUN%' THEN 'NUNSHEN'
      WHEN a.code_analytique LIKE 'HOR%' THEN 'COMMERCE'
      WHEN a.code_analytique LIKE 'OFF%' THEN 'COMMERCE'
      WHEN a.code_analytique LIKE 'NES%' THEN 'NESHU'
      WHEN a.code_analytique LIKE 'SAV%' THEN 'TECHNIQUE'
      WHEN a.code_analytique LIKE 'COM%' THEN 'COMMERCE'
      WHEN a.code_analytique LIKE 'PDET%' THEN 'PIECES DET'
      ELSE NULL
    END AS code_analytique_bu,

    c.numero_compte_general,
    c.libelle_ecriture,
    cbu.macro_categorie_pnl_bu,

    c.sens_ecriture,

    a.montant_analytique,

    c.date_facturation,
    c.date_ecriture_comptable,
    c.date_periode_facturation,
    c.jour_facturation,

    -- Indicateur qualité : écriture sans ligne analytique
    CASE 
      WHEN a.numero_ecriture_comptable IS NULL THEN TRUE 
      ELSE FALSE 
    END AS is_missing_analytical,

    -- Métadonnées
    COALESCE(a.created_at, c.created_at) AS created_at,
    COALESCE(a.updated_at, c.updated_at) AS updated_at

  FROM ecritures_comptables c
  LEFT JOIN ecritures_analytiques a
    ON c.numero_ecriture_comptable = a.numero_ecriture_comptable
  LEFT JOIN mapping_code_analytique__bu bu
    ON a.code_analytique = bu.code_analytique
  LEFT JOIN mapping_code_comptable__bu cbu
    ON CAST(c.numero_compte_general AS STRING) = cbu.code_comptable
)

-- ✅ Calcul des indicateurs qualité APRÈS jointure et fallback
SELECT
  numero_ecriture_comptable,
  numero_plan_analytique,
  numero_ligne_analytique,

  code_analytique,
  code_analytique_bu,

  numero_compte_general,
  libelle_ecriture,
  macro_categorie_pnl_bu,

  sens_ecriture,

  montant_analytique,

  CASE
  WHEN LEFT(CAST(numero_compte_general AS STRING), 1) = '6' AND sens_ecriture = 0 THEN -ABS(montant_analytique)  -- charge débitée → négatif
  WHEN LEFT(CAST(numero_compte_general AS STRING), 1) = '6' AND sens_ecriture = 1 THEN ABS(montant_analytique)   -- charge créditée → positif (rare, extourne)
  WHEN LEFT(CAST(numero_compte_general AS STRING), 1) = '7' AND sens_ecriture = 0 THEN -ABS(montant_analytique)  -- produit débité (rare, correction)
  WHEN LEFT(CAST(numero_compte_general AS STRING), 1) = '7' AND sens_ecriture = 1 THEN ABS(montant_analytique)   -- produit crédité → positif
  ELSE montant_analytique
  END AS montant_analytique_signe,

  date_facturation,
  date_ecriture_comptable,
  date_periode_facturation,
  jour_facturation,

  is_missing_analytical,
  
  -- ✅ Calcul des flags APRÈS le fallback
  CASE
    WHEN code_analytique_bu IS NULL AND is_missing_analytical = FALSE THEN TRUE
    ELSE FALSE
  END AS is_missing_bu_mapping,

  CASE
    WHEN macro_categorie_pnl_bu IS NULL THEN TRUE
    ELSE FALSE
  END AS is_missing_comptable_mapping,

  created_at,
  updated_at
  
FROM mapped_with_fallback
    );
  