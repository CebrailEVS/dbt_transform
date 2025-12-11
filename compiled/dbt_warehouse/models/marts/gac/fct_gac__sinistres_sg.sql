
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
'220cc5af-2a03-4652-9be2-67f27a6a5638' as dbt_invocation_id
 FROM `evs-datastack-prod`.`prod_staging`.`stg_gac__sinistres_sg`