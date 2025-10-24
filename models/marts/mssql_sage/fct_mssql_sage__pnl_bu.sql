{{ config(
    materialized = 'table',
    unique_key = ['numero_ecriture_comptable', 'numero_plan_analytique', 'numero_ligne_analytique'],
    partition_by = {
        'field': 'date_facturation',
        'data_type': 'timestamp'
    },
    cluster_by = ['numero_compte_general', 'code_section_analytique']
) }}

WITH ecritures_comptables AS (
  SELECT
    ec_no AS numero_ecriture_comptable,
    cg_num AS numero_compte_general,
    ec_intitule AS libelle_ecriture,
    ec_date AS date_ecriture_comptable,
    jm_date AS date_periode_facturation,
    ec_jour AS jour_facturation,

    -- Reconstruction de la date de facturation complète
    timestamp(DATE_ADD(jm_date, INTERVAL (ec_jour - 1) DAY)) AS date_facturation,

    created_at,
    updated_at
  FROM {{ ref('stg_mssql_sage__f_ecriturec') }}
  WHERE LEFT(CAST(cg_num AS STRING), 1) IN ('6', '7')
),

ecritures_analytiques AS (
  SELECT
    ec_no AS numero_ecriture_comptable,
    n_analytique AS numero_plan_analytique,
    ea_ligne AS numero_ligne_analytique,
    ca_num AS code_section_analytique,
    ea_montant AS montant_analytique,
    created_at,
    updated_at
  FROM {{ ref('stg_mssql_sage__f_ecriturea') }}
)

SELECT
  -- PK composite (clé de l'écriture analytique)
  c.numero_ecriture_comptable,
  a.numero_plan_analytique,
  a.numero_ligne_analytique,
  
  -- Dimensions analytiques
  a.code_section_analytique,
  
  -- Comptabilité générale
  c.numero_compte_general,
  c.libelle_ecriture,
  
  -- Montants
  a.montant_analytique,
  
  -- Dates (ordonnées par importance métier)
  c.date_facturation,              
  c.date_ecriture_comptable,       
  c.date_periode_facturation,      
  c.jour_facturation,
  
  -- Indicateurs qualité
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