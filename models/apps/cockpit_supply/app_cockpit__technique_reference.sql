{{ config(materialized='table') }}

-- Références connues du stock TechCare sur tout l'historique : l'app s'en sert pour
-- retrouver la référence complète (EVS_NESPRESSO_ / EVS_NESP_) d'un code court.
select distinct reference
from {{ ref('fct_supply_chain__stock_yuman') }}
