
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_yuman_gcs__stock_articles`
      
    
    

    
    OPTIONS(
      description="""Table marts des suivi inventaire th\u00e9orique des pi\u00e8ces/articles Yuman depuis la table staging afin de suivre l'\u00e9volution des stocks des techniciens et d\u00e9p\u00f4ts Yuman"""
    )
    as (
      

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
    '15a963fc-e2ec-4a0c-a29b-f2695bfa964e' as dbt_invocation_id

    FROM `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    WHERE reference is not null and nom_du_stock is not null
)

SELECT * 
FROM filtered_stocks
    );
  