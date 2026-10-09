
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_detail`
      
    partition by timestamp_trunc(date_heure_debut, month)
    cluster by product_reference

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Pi\u00e8ces Nespresso pos\u00e9es par les techniciens TechCare, ligne \u00e0 ligne.\n[COMMENT CONSTRUITE] M\u00eames filtres que app_cockpit__technique_conso_nespresso_mensuelle, sans agr\u00e9gation ; r\u00e9f\u00e9rence normalis\u00e9e en product_reference, code_article brut conserv\u00e9.\n[GRAIN] 1 ligne par pi\u00e8ce pos\u00e9e lors d'une intervention (celui de la source).\n[NOTES] Jamais charg\u00e9e en m\u00e9moire : l'app l'interroge \u00e0 la demande (d\u00e9tail d'un mois pour l'export du flux TechCare, conso jour par jour d'une r\u00e9f\u00e9rence sur 12 mois glissants). Partition mensuelle sur date_heure_debut, clustering product_reference. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Pièces Nespresso posées, ligne à ligne, mêmes filtres que la table mensuelle :
-- jamais chargée en mémoire, interrogée à la demande par l'app (détail d'un mois pour
-- l'export du flux TechCare, conso jour par jour d'une référence sur 12 mois).
select
    date_heure_debut,
    code_article,
    coalesce(upper(trim(product_reference)), 'NONE') as product_reference,
    technician_id,
    nom_article,
    coalesce(qty_consommee, 0) as qty_consommee
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
where
    etat_intervention in ('terminée signée', 'terminée non signée')
    and date_heure_debut is not null
    and coalesce(upper(trim(code_article)), 'NONE') not in ('0000001', '0000100')
    );
  