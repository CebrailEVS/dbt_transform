

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

consultations as (
    select
        date(e.created_at) as activite_date,
        p.report_id,
        count(*) as nb_consultations,
        count(distinct e.user_id) as nb_utilisateurs_distincts
    from evenements as e
    inner join parc as p on e.report_id = p.report_id
    where e.activity = 'ViewReport'
    group by 1, 2
),

-- Les rafraichissements sont portes par le MODELE SEMANTIQUE, pas par le
-- rapport. On les rattache au rapport via dataset_id, ce qui n'est juste que
-- parce que le parc est en 1:1 rapport/modele. Cette hypothese est verrouillee
-- par le test `unique` sur dim_bi__rapport.dataset_id : si un modele venait a
-- servir deux rapports, le test casse AVANT que les mesures soient doublees.
rafraichissements as (
    select
        date(e.created_at) as activite_date,
        p.report_id,
        count(*) as nb_rafraichissements
    from evenements as e
    inner join parc as p on e.dataset_id = p.dataset_id
    where e.activity = 'RefreshDataset'
    group by 1, 2
),

activite as (
    select
        coalesce(c.activite_date, r.activite_date) as activite_date,
        coalesce(c.report_id, r.report_id) as report_id,
        coalesce(c.nb_consultations, 0) as nb_consultations,
        coalesce(c.nb_utilisateurs_distincts, 0) as nb_utilisateurs_distincts,
        coalesce(r.nb_rafraichissements, 0) as nb_rafraichissements
    from consultations as c
    -- FULL OUTER : un jour peut porter un rafraichissement sans consultation
    -- (c'est precisement le cas des rapports dormants) et l'inverse.
    full outer join rafraichissements as r
        on
            c.activite_date = r.activite_date
            and c.report_id = r.report_id
)

select
    -- grain : 1 ligne = 1 (jour x rapport) ayant porte au moins une activite
    a.activite_date,
    a.report_id,

    -- attributs d'affichage aplatis du parent direct (marts.md § 1)
    p.report_name,
    p.workspace_name,

    -- mesures
    a.nb_consultations,
    a.nb_utilisateurs_distincts,
    a.nb_rafraichissements

from activite as a
inner join parc as p on a.report_id = p.report_id