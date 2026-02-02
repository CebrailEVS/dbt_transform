
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_nesp_tech__pieces_detachees_pricing`
      
    
    

    
    OPTIONS(
      description="""Ce mod\u00e8le combine les interventions termin\u00e9es/sign\u00e9es issues des agences EVS  avec les consommations d\u2019articles techniques et leurs tarifs.  Il permet de suivre les ventes d\u2019articles par technicien, par jour et par intervention.\n"""
    )
    as (
      

-- Modèle : interventions_articles
-- Objectif : ce modèle combine les interventions terminées/signées avec les articles consommés
-- et leurs informations tarifaires, afin de calculer les montants totaux par article.
-- Il permet d’obtenir un niveau de détail par intervention (n_planning) et article.

with inters as (
  -- Étape 1 : Sélection des interventions terminées ou signées, filtrées par les agences EVS
  -- NB : on récupére les dates de fin de la partie Interventions car pour les Pièces, c'est la date de début qui fait foi.
  select 
    n_planning,
    cast(date_heure_fin as date) as date_fin
  from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__interventions`
  where etat_intervention in ('terminée signée','signature différée')
    and agency in ('evs','evs paris','evs idf','evs paris 2')
),
final as (
  -- Étape 2 : Jointure avec les consommations d’articles et leurs prix
  select 
    i.n_planning,
    i.date_fin,
    extract(year from i.date_fin) as annee,
    extract(month from i.date_fin) as mois,
    extract(day from i.date_fin) as jour,
    n_tech,
    nom_tech,
    prenom_tech,
    art_pricing.article_ref_nomad as piece_ref_nomad,
    art_pricing.article_desc as piece_desc,
    quantite_article as piece_quantite,
    article_prix_unitaire as piece_prix_unitaire,
    (quantite_article * article_prix_unitaire) as montant_total
  from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles` art_conso
  left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix` art_pricing
    on art_conso.code_article = lower(art_pricing.article_ref_nomad)
  inner join inters i 
    on i.n_planning = art_conso.n_planning
)
-- Étape 3 : Sélection finale
SELECT
-- Info Interventions et Technicien
n_planning,
date_fin,
n_tech,
nom_tech,
prenom_tech,
-- Info Piece detachees
piece_ref_nomad,
piece_desc,
piece_prix_unitaire,
piece_quantite,
-- Montant a facturer
montant_total,
-- Métadonnées dbt
CURRENT_TIMESTAMP() as dbt_updated_at,
'2b298925-72a3-4e82-8ae1-ce16ed15f635' as dbt_invocation_id
FROM final
    );
  