

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
    'ea4d8be6-666f-45c2-a1cd-701198274b47' as dbt_invocation_id

    FROM `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    WHERE reference is not null and nom_du_stock is not null
)

SELECT * 
FROM filtered_stocks