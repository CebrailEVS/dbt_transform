{{ config(
    materialized='table',
    cluster_by=['report_id']
) }}

with parc as (
    select
        report_id,
        dataset_id,
        report_name,
        workspace_name
    from {{ ref('dim_bi__rapport') }}
),

evenements as (
    select * from {{ ref('stg_powerbi_activity__events') }}
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
