
  
    

    create or replace table `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_piece_nespresso`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nR\u00e9f\u00e9rentiel des pi\u00e8ces Nespresso qu'un manager peut ajouter \u00e0 une\nintervention lors d'un retraitement de pi\u00e8ces d\u00e9tach\u00e9es, avec leur prix\ncatalogue. Exclut les lignes de saisie d'intervention (d\u00e9tartrage,\n\u00ab pas de pi\u00e8ce \u00bb, pr\u00e9ventives...) qui ne sont pas des pi\u00e8ces.\n\n[COMMENT CONSTRUITE]\nArticles Yuman (`stg_yuman__products`) dont la r\u00e9f\u00e9rence commence par\n`EVS_NESPRESSO_`, actifs et non interdits, hors pseudo-articles du seed\n`ref_yuman__pseudo_article`. `code_article` est la r\u00e9f\u00e9rence sans pr\u00e9fixe, en\nmajuscules. Le prix vient du seed `ref_nesp_tech__articles_prix`, joint comme\ndans `fct_technique__piece_detachee_pricing_nespresso`\n(`lower(code_article) = lower(article_ref_nomad)`).\n\n[GRAIN]\n1 ligne par `code_article`.\n\n[NOTES]\nLu par la page Retraitements Pi\u00e8ces D\u00e9tach\u00e9es de l'application Suivi Tech\n(liste des pi\u00e8ces ajoutables). R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI\ndessus. Yuman distingue la casse : si deux fiches ne diff\u00e8rent que par la casse\ndu code, la r\u00e9f\u00e9rence en majuscules est conserv\u00e9e, \u00e0 d\u00e9faut la plus ancienne.\n`prix_unitaire` est null pour une pi\u00e8ce absente du seed de prix ou dont le\ntarif y vaut 0 (r\u00e9f\u00e9rences \u00ab Non R\u00e9f\u00e9renc\u00e9 CORDON \u00bb) : un prix \u00e0 0 est un\ntarif inconnu, pas une pi\u00e8ce gratuite.\n"""
    )
    as (
      

with pseudo_articles as (
    select reference from `evs-datastack-prod`.`prod_reference`.`ref_yuman__pseudo_article`
),

pieces as (
    select
        p.product_code as product_reference,
        p.product_name as libelle,
        upper(substr(p.product_code, length('EVS_NESPRESSO_') + 1)) as code_article,
        p.product_id
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__products` as p
    left join pseudo_articles as pa
        on p.product_code = pa.reference
    where
        starts_with(p.product_code, 'EVS_NESPRESSO_')
        and p.is_active
        and not coalesce(p.is_forbidden_article, false)
        and pa.reference is null
),

-- Yuman distingue la casse : deux fiches peuvent porter le même code ne différant que par
-- la casse. On garde la référence en majuscules (celle des marts de consommation Nespresso),
-- à défaut la plus ancienne.
pieces_dedup as (
    select
        code_article,
        product_reference,
        libelle
    from pieces
    qualify row_number() over (
        partition by code_article
        order by (product_reference = 'EVS_NESPRESSO_' || code_article) desc, product_id asc
    ) = 1
)

select
    pieces_dedup.code_article,
    pieces_dedup.product_reference,
    pieces_dedup.libelle,
    -- Un prix à 0 du seed (« Non Référencé CORDON ») est un tarif inconnu, pas une pièce gratuite.
    nullif(prix.article_prix_unitaire, 0) as prix_unitaire
from pieces_dedup
-- Même jointure que fct_technique__piece_detachee_pricing_nespresso.
left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix` as prix
    on lower(pieces_dedup.code_article) = lower(prix.article_ref_nomad)
    );
  