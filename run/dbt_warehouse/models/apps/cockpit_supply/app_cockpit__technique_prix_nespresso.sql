
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_prix_nespresso`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Prix unitaire de chaque pi\u00e8ce d\u00e9tach\u00e9e Nespresso (syst\u00e8me Nomad), utilis\u00e9 par Cockpit Supply pour valoriser la consommation et le stock TechCare.\n[COMMENT CONSTRUITE] Couples distincts (piece_ref_nomad, piece_prix_unitaire) du seed ref_nesp_tech__articles_prix pour les pi\u00e8ces pos\u00e9es lors des interventions \u00ab termin\u00e9e sign\u00e9e \u00bb / \u00ab signature diff\u00e9r\u00e9e \u00bb des agences EVS (int_nesp_tech__articles_dedup, int_nesp_tech__interventions_dedup), prix renseign\u00e9s.\n[GRAIN] 1 ligne par piece_ref_nomad (un seul prix par r\u00e9f\u00e9rence dans la source).\n[NOTES] Les noms et num\u00e9ros des techniciens, inutiles \u00e0 l'app, ne sont pas repris. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Prix unitaire des pièces Nespresso (système Nomad), un par référence, pour les pièces posées lors des
-- interventions terminées des agences EVS. Un seul prix par référence dans le référentiel de prix.
select distinct
    p.article_ref_nomad as piece_ref_nomad,
    p.article_prix_unitaire as piece_prix_unitaire
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup` as a
inner join `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as i
    on a.n_planning = i.n_planning
inner join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix` as p
    on a.code_article = lower(p.article_ref_nomad)
where
    i.etat_intervention in ('terminée signée', 'signature différée')
    and i.agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
    and p.article_prix_unitaire is not null
    );
  