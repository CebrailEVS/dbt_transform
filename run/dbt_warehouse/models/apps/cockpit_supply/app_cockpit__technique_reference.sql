
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_reference`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rences connues du stock TechCare sur tout l'historique. Sert \u00e0 Cockpit Supply pour retrouver la r\u00e9f\u00e9rence compl\u00e8te (EVS_NESPRESSO_ ou EVS_NESP_) d'un code article court, et donc son prix.\n[COMMENT CONSTRUITE] R\u00e9f\u00e9rences distinctes de fct_supply_chain__stock_yuman.\n[GRAIN] 1 ligne par reference.\n[NOTES] Toutes zones et tous jours : une r\u00e9f\u00e9rence vue un seul jour compte (elle change le pr\u00e9fixe retenu). R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Références connues du stock TechCare sur tout l'historique : l'app s'en sert pour
-- retrouver la référence complète (EVS_NESPRESSO_ / EVS_NESP_) d'un code court.
select distinct reference
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`
    );
  