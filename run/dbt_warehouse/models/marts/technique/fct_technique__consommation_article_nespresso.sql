
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
      
    partition by timestamp_trunc(date_heure_debut, day)
    cluster by product_reference, technician_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Consommation d'articles (pi\u00e8ces d\u00e9tach\u00e9es, consommables) lors des interventions techniques Nespresso (Nomad Repair) \u2014 le pendant Nespresso de `fct_technique__consommation_article_yuman` : ces interventions ne passent pas par les bons Yuman, mais leurs articles sont centralis\u00e9s dans le r\u00e9f\u00e9rentiel Yuman sous le pr\u00e9fixe `EVS_NESPRESSO_` (colonne product_reference = cl\u00e9 de rapprochement avec les stocks Yuman).\n[COMMENT CONSTRUITE] `int_nesp_tech__articles_dedup` (une ligne = un article pos\u00e9 sur une intervention), joint (inner join) \u00e0 `int_nesp_tech__interventions_dedup` restreint aux agences EVS ('evs', 'evs paris', 'evs idf', 'evs paris 2' \u2014 'nespresso sud' = sous-traitant hors flux de stock EVS, plus actif depuis 2025) pour l'\u00e9tat, l'agence et les horodatages d\u00e9but/fin. technician_id r\u00e9solu via le matricule Nomad (n_tech \u2194 dim_technique__technician.nomad_id, unique). product_reference = 'EVS_NESPRESSO_' + upper(code_article).\n[GRAIN] 1 ligne par (n_planning, code_article). ~168k lignes depuis nov. 2023.\n[NOTES] Aucun filtre d'\u00e9tat : une ligne de la table articles = une pose r\u00e9elle (valid\u00e9 m\u00e9tier ; 99,95 % des lignes portent un \u00e9tat termin\u00e9), etat_intervention est expos\u00e9 en information. Contient les lignes de saisie non stockables (D\u00e9tartrage '0000100', Pas de pi\u00e8ce '0000001', flag 'miniprev') \u2014 \u00e0 exclure selon l'usage via le seed ref_yuman__pseudo_article (sur product_reference). Source extraite chaque lundi (fen\u00eatre 7 jours glissants) : fra\u00eecheur hebdomadaire, pas quotidienne. Flux disjoint des bons Yuman (outils non m\u00e9lang\u00e9s, v\u00e9rifi\u00e9 m\u00e9tier).\n"""
    )
    as (
      

-- Pendant Nespresso (Nomad Repair) de fct_technique__consommation_article_yuman :
-- les interventions Nespresso ne passent pas par les bons Yuman, mais leurs articles
-- sont centralisés dans le référentiel Yuman sous le préfixe EVS_NESPRESSO_.
-- Une ligne de la table articles = une pose réelle (vérifié : 99,95 % des lignes
-- portent un état terminé) — aucun filtre d'état, l'état est exposé en information.

with articles as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup`
),

interventions as (
    -- Contexte d'intervention : états, agence, horodatages début/fin.
    -- Périmètre agences EVS uniquement ('nespresso sud' = sous-traitant,
    -- hors flux de stock EVS, plus actif depuis 2025).
    select
        n_planning,
        etat_intervention,
        agency,
        date_heure_debut,
        date_heure_fin
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
    where agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
),

technicians as (
    -- Rattachement au référentiel technicien Yuman via le matricule Nomad
    -- (nomad_id unique dans la dim, pas de fan-out).
    select
        user_id,
        nomad_id
    from `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`
    where nomad_id is not null
)

select
    -- Grain
    a.n_planning,
    a.code_article,

    -- FK dimensions
    t.user_id as technician_id,

    -- Attributs d'affichage
    concat('EVS_NESPRESSO_', upper(a.code_article)) as product_reference,
    a.nom_article,
    a.n_tech,
    a.nom_tech,
    a.prenom_tech,
    a.n_client,
    a.raison_sociale_client,
    a.n_site,
    a.nom_site,
    a.code_machine,
    a.nom_machine,
    a.num_serie_machine,

    -- État métier de l'intervention (information, non filtré)
    i.etat_intervention,
    i.agency,

    -- Dates d'intervention (horodatages iso interventions_dedup)
    i.date_heure_debut,
    i.date_heure_fin,

    -- Mesure
    a.quantite_article as qty_consommee,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '1cc5fdb6-a2a0-4ca7-8a04-d804baab3fd8' as dbt_invocation_id
from articles as a
inner join interventions as i
    on a.n_planning = i.n_planning
left join technicians as t
    on lower(a.n_tech) = lower(cast(t.nomad_id as string))
    );
  