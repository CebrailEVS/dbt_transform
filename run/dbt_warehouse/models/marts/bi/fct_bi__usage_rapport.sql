
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_bi__usage_rapport`
      
    
    cluster by report_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] \u00c9tat d'usage de chaque rapport du parc sur la fen\u00eatre disponible : combien de fois lu, par combien de personnes, quand pour la derni\u00e8re fois, et combien de rafra\u00eechissements il a co\u00fbt\u00e9. R\u00e9pond \u00e0 la question \u00ab quels rapports peut-on arr\u00eater, et qu'est-ce qu'on y gagne \u00bb.\n[COMMENT CONSTRUITE] Le parc de dim_bi__rapport en LEFT JOIN sur deux agr\u00e9gats de stg_powerbi_activity__events (consultations par report_id, rafra\u00eechissements par dataset_id). Le LEFT JOIN est essentiel : il conserve les rapports sans aucune consultation, qui sont l'objet m\u00eame du mart. Construit depuis les \u00e9v\u00e9nements et non par rollup de fct_bi__activite_rapport_jour, car `nb_utilisateurs_distincts` n'est pas additif.\n[GRAIN] 1 ligne par report_id (PK) \u2014 le parc entier, dormants inclus. 37 lignes au 2026-09-03, dont 18 dormantes.\n[NOTES] Fait de type snapshot, au grain de dim_bi__rapport (1:1). Les mesures couvrent toute la fen\u00eatre pr\u00e9sente dans prod_raw, pas une p\u00e9riode glissante fixe : l'API ne conserve que 27 jours et prod_raw est la seule archive, donc la profondeur cro\u00eet avec l'anciennet\u00e9 de la collecte. `nb_jours_depuis_derniere_consultation` est fig\u00e9 \u00e0 la date du build (`current_date()`). Rep\u00e8re m\u00e9tier au 2026-09-03 : 312 rafra\u00eechissements pour z\u00e9ro lecteur, et 8 rapports \u00e0 la fois dormants et jamais rafra\u00eechis \u2014 ceux-l\u00e0 sont morts, pas \u00e0 optimiser.\n"""
    )
    as (
      

with parc as (
    select
        report_id,
        dataset_id,
        report_name,
        workspace_name
    from `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
),

evenements as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
),

-- Construit depuis les evenements et NON par rollup de
-- fct_bi__activite_rapport_jour : `nb_utilisateurs_distincts` n'est pas
-- additif, la somme des distincts journaliers compterait plusieurs fois la
-- meme personne revenue un autre jour.
consultations as (
    select
        p.report_id,
        count(*) as nb_consultations,
        count(distinct e.user_id) as nb_utilisateurs_distincts,
        count(distinct date(e.created_at)) as nb_jours_actifs,
        max(e.created_at) as derniere_consultation_at
    from evenements as e
    inner join parc as p on e.report_id = p.report_id
    where e.activity = 'ViewReport'
    group by 1
),

-- Rattachement via dataset_id : voir la note de
-- fct_bi__activite_rapport_jour sur l'hypothese 1:1 rapport/modele.
rafraichissements as (
    select
        p.report_id,
        count(*) as nb_rafraichissements
    from evenements as e
    inner join parc as p on e.dataset_id = p.dataset_id
    where e.activity = 'RefreshDataset'
    group by 1
)

select
    -- grain : 1 ligne = 1 rapport du parc metier (tout le parc, dormants inclus)
    p.report_id,

    -- attributs d'affichage aplatis du parent direct (marts.md § 1)
    p.report_name,
    p.workspace_name,

    -- date metier
    c.derniere_consultation_at,

    -- booleens
    coalesce(c.nb_consultations, 0) = 0 as is_dormant,

    -- mesures
    coalesce(c.nb_consultations, 0) as nb_consultations,
    coalesce(c.nb_utilisateurs_distincts, 0) as nb_utilisateurs_distincts,
    coalesce(c.nb_jours_actifs, 0) as nb_jours_actifs,
    coalesce(r.nb_rafraichissements, 0) as nb_rafraichissements,
    date_diff(current_date(), date(c.derniere_consultation_at), day)
        as nb_jours_depuis_derniere_consultation

from parc as p
-- LEFT : le parc entier doit sortir, y compris les rapports jamais consultes.
-- C'est tout l'objet de ce mart.
left join consultations as c on p.report_id = c.report_id
left join rafraichissements as r on p.report_id = r.report_id
    );
  