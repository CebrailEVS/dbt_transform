
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__piece_detachee_pricing_nespresso`
      
    partition by date_fin
    cluster by n_tech

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nPricing des pi\u00e8ces d\u00e9tach\u00e9es consomm\u00e9es lors des interventions Nespresso\n(Nomad Repair). Suit les ventes d'articles techniques par technicien, par\njour et par intervention.\n\n[COMMENT CONSTRUITE]\nFiltre les interventions Nespresso termin\u00e9es/sign\u00e9es issues des agences\nEVS (evs, evs paris, evs idf, evs paris 2) depuis\n`int_nesp_tech__interventions_dedup`. Joint aux consommations d'articles\ntechniques et leur tarif catalogue.\n\n[GRAIN]\n1 ligne par (intervention `n_planning`, article consomm\u00e9).\n\n[NOTES]\nSource nesp_tech = fichier Excel hebdomadaire (Mon 07:30). Suffixe\n`_nespresso` au nom du mart pour distinguer de la tarification Yuman\n(tous partenaires), d\u00e9sormais port\u00e9e par `int_yuman__interventions`.\n"""
    )
    as (
      

with inters as (
    select
        n_planning,
        cast(date_heure_fin as date) as date_fin
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup`
    where
        etat_intervention in ('terminée signée', 'signature différée')
        and agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
),

final as (
    select
        i.n_planning,
        i.date_fin,
        extract(year from i.date_fin) as annee,
        extract(month from i.date_fin) as mois,
        extract(day from i.date_fin) as jour,
        art_conso.n_tech,
        art_conso.nom_tech,
        art_conso.prenom_tech,
        art_pricing.article_ref_nomad as piece_ref_nomad,
        art_pricing.article_desc as piece_desc,
        art_conso.quantite_article as piece_quantite,
        art_pricing.article_prix_unitaire as piece_prix_unitaire,
        (art_conso.quantite_article * art_pricing.article_prix_unitaire) as montant_total
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup` as art_conso
    left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix` as art_pricing
        on art_conso.code_article = lower(art_pricing.article_ref_nomad)
    inner join inters as i
        on art_conso.n_planning = i.n_planning
)

select
    -- Info Interventions et Technicien
    n_planning,
    date_fin,
    n_tech,
    nom_tech,
    prenom_tech,

    -- Info Pieces detachees
    piece_ref_nomad,
    piece_desc,
    piece_prix_unitaire,
    piece_quantite,

    -- Montant a facturer
    montant_total,

    -- Metadonnees dbt
    current_timestamp() as dbt_updated_at,
    '0d976550-ce53-462f-8457-0db1c951a8ba' as dbt_invocation_id

from final
    );
  