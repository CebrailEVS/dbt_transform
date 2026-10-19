
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_ressource`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9f\u00e9rentiel des ressources Caf\u00e9s du Phare (v\u00e9hicules), tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] stg_oracle_lcdp__resources de type PERSON ou VEHICLE (stg_oracle_lcdp__resources_type). Colonnes lues par l'app seulement.\n[GRAIN] 1 ligne par resources_id.\n[NOTES] lue via get_dim_resources_lcdp ; \u00e9crans : _flux, lcdp_distrib, lcdp_torrefaction, supply. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Référentiel des ressources Cafés du Phare (personnes et véhicules) pour Cockpit Supply, construit depuis le staging.
select
    r.idresources as resources_id,
    r.code as resources_code,
    r.name as resources_name,
    rt.code as resources_type
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources` as r
inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources_type` as rt
    on r.idresources_type = rt.idresources_type
where rt.code in ('PERSON', 'VEHICLE')
    );
  