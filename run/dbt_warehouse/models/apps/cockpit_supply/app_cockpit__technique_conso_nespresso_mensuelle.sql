
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_mensuelle`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Pi\u00e8ces Nespresso pos\u00e9es par les techniciens TechCare, par mois, r\u00e9f\u00e9rence et technicien. Base de tous les calculs mensuels de Cockpit Supply sur Nespresso : consommation par d\u00e9p\u00f4t et par technicien, index saisonnier, facturation, forecast, ABC/XYZ.\n[COMMENT CONSTRUITE] fct_technique__consommation_article_nespresso filtr\u00e9e comme l'app (interventions \u00ab termin\u00e9e sign\u00e9e \u00bb / \u00ab termin\u00e9e non sign\u00e9e \u00bb, date renseign\u00e9e, codes de main-d'\u0153uvre 0000001 et 0000100 exclus), agr\u00e9g\u00e9e par mois (date_heure_debut en UTC), r\u00e9f\u00e9rence normalis\u00e9e (majuscules, sans espaces) et technician_id ; quantit\u00e9 somm\u00e9e, libell\u00e9 (unique par mois et r\u00e9f\u00e9rence).\n[GRAIN] 1 ligne par (mois, product_reference, technician_id), technician_id NULL compris.\n[NOTES] Le rattachement technicien \u2192 d\u00e9p\u00f4t / nom et la valorisation restent dans l'app (r\u00e9f\u00e9rentiels et prix hors BigQuery) ; l'app regroupe ensuite au grain (mois, r\u00e9f\u00e9rence, d\u00e9p\u00f4t). R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Pièces Nespresso posées par les techniciens, par mois, référence et technicien : grain
-- de tous les calculs mensuels de l'app (conso par dépôt, par technicien, index
-- saisonnier, facturation). Filtres repris de l'app (data/csv_source.py) :
-- interventions réalisées, date renseignée, codes de main-d'œuvre exclus. Le mois est
-- celui de date_heure_debut en UTC, comme l'app. Le technicien → dépôt / nom et le
-- prix restent calculés par l'app (sources hors BigQuery).
select
    format_timestamp('%Y-%m', date_heure_debut) as mois,
    coalesce(upper(trim(product_reference)), 'NONE') as product_reference,
    technician_id,
    coalesce(sum(qty_consommee), 0) as qte,
    max(nom_article) as nom_article
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
where
    etat_intervention in ('terminée signée', 'terminée non signée')
    and date_heure_debut is not null
    and coalesce(upper(trim(code_article)), 'NONE') not in ('0000001', '0000100')
group by mois, product_reference, technician_id
    );
  