
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_gac__sinistres_sg`
      
    
    

    
    OPTIONS(
      description="""Donn\u00e9es de sinistres issues du mod\u00e8le stg_gac__sinistres_sg"""
    )
    as (
      
-- ============================================================================
-- MODEL: fct_gac__sinistres_sg
-- PURPOSE: Table de faits sur les sinistres vehicules
-- AUTHOR: Etienne BOULINIER
-- ============================================================================
SELECT 
-- Infos Sinistre
n_de_sinistre,
DATE(date_sinistre) as date_sinistre,
date_de_creation,
circonstance,
tiers,
resp,
cloture,
-- Infos Vehicule
immat,
nom,
prenom,
statut_actuel,
genre_fiscal,
reference_gac,
entite_entite_2,
entite_entite_3,
--Couts
centre_de_couts,
cout_assureur,
auto_assurance,
franchise,
cout_global,
cout_client,
-- Métadonnées dbt
CURRENT_TIMESTAMP() as dbt_updated_at,
'ea1f52b4-b1c6-4f1c-8686-2248dc85468a' as dbt_invocation_id
 FROM `evs-datastack-prod`.`prod_staging`.`stg_gac__sinistres_sg`
    );
  