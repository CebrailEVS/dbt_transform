{{
  config(
    materialized='table',
    description='Table de faits des stocks des pièces Yuman journaliers pour chaque stock technicien et dépôt'
  )
}}

-- ============================================================================
-- MODEL: fct_yuman__stocks
-- PURPOSE: Suivis des stocks pièces Yuman par stock technicien et dépôt journaliers
-- AUTHOR: Cebrail AKSOY
-- ============================================================================

WITH 
-- ============================================================================
-- 1. BASE DATA EXTRACTION
-- ============================================================================
filtered_stocks as (

SELECT
    -- Attributs métier 
    reference, 
    designation, 
    nom_du_stock as stock,

    -- Mesure
    quantite, 

    -- Date
    DATE(export_date) as stock_date,

    -- Métadonnées d'exécution
    CURRENT_TIMESTAMP() as dbt_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id

    FROM {{ ref('stg_yuman_gcs__stock_theorique') }}
    WHERE reference is not null and nom_du_stock is not null
)

SELECT * 
FROM filtered_stocks