

-- Références connues du stock TechCare sur tout l'historique : l'app s'en sert pour
-- retrouver la référence complète (EVS_NESPRESSO_ / EVS_NESP_) d'un code court.
select distinct reference
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`