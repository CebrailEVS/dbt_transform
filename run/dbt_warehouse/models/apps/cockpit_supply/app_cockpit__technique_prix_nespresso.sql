
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_prix_nespresso`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Prix unitaire de chaque pi\u00e8ce d\u00e9tach\u00e9e Nespresso (syst\u00e8me Nomad), utilis\u00e9 par Cockpit Supply pour valoriser la consommation et le stock TechCare.\n[COMMENT CONSTRUITE] Couples distincts (piece_ref_nomad, piece_prix_unitaire) de fct_technique__piece_detachee_pricing_nespresso, prix renseign\u00e9s.\n[GRAIN] 1 ligne par piece_ref_nomad (un seul prix par r\u00e9f\u00e9rence dans la source).\n[NOTES] Les noms et num\u00e9ros des techniciens, inutiles \u00e0 l'app, ne sont pas repris. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Prix unitaire des pièces Nespresso (système Nomad), un par référence : c'est tout ce
-- que l'app lit de cette table. Un seul prix par référence dans la source.
select distinct
    piece_ref_nomad,
    piece_prix_unitaire
from `evs-datastack-prod`.`prod_marts`.`fct_technique__piece_detachee_pricing_nespresso`
where piece_prix_unitaire is not null
    );
  