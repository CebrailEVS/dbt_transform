{{ config(
    materialized='table'
) }}

-- Aucune colonne de ce mart ne doit porter un nom déjà présent dans
-- fct_technique__intervention_retraitee : l'application Suivi Tech joint les deux modèles
-- et sélectionne ses colonnes sans qualificatif de table.

-- Population : interventions en alerte de consommation (alerte_compile non vide) ou avec une
-- pose sans stock (quantité posée d'une pièce contrôlée supérieure au stock du van la veille).
-- Les alertes viennent de l'intermédiaire partagé avec le mart Power BI ; la règle de pose sans
-- stock vit ici seule. BigQuery recalcule une CTE à chaque lecture : les CTE lourdes (pièces,
-- stock) ne sont lues qu'une fois, la règle se calcule dans l'agrégation finale.
with alertes_int as (
    select
        n_planning,
        alerte_compile
    from {{ ref('int_nesp_tech__alerte_conso_aguila') }}
    where coalesce(alerte_compile, '') != ''
),

-- Un nomad_id = un van. dim_technique__technician ne garantit pas l'unicité de nomad_id :
-- on garde la fiche active, à défaut la plus récemment mise à jour.
tech as (
    select
        lower(nomad_id) as nomad_id,
        storehouses_name
    from {{ ref('dim_technique__technician') }}
    where nomad_id is not null
    qualify row_number() over (
        partition by lower(nomad_id)
        order by is_active desc, updated_at desc
    ) = 1
),

-- Interventions portant au moins une pièce contrôlée.
avec_piece_controlee as (
    select distinct ar.n_planning
    from {{ ref('int_nesp_tech__articles_dedup') }} as ar
    inner join {{ ref('ref_nesp_tech__piece_controle_stock') }} as pc
        on upper(ar.code_article) = pc.code_article
),

-- Interventions candidates, hors annulées ou en échec, tous types confondus : celles en alerte
-- et celles qui portent une pièce contrôlée. date_pose : jour de fin d'intervention.
candidats as (
    select
        i.n_planning,
        a.alerte_compile as alerte_int,
        cast(i.date_heure_fin as date) as date_pose,
        t.storehouses_name as van_technicien
    from {{ ref('int_nesp_tech__interventions_dedup') }} as i
    left join alertes_int as a
        on i.n_planning = a.n_planning
    left join avec_piece_controlee as k
        on i.n_planning = k.n_planning
    left join tech as t
        on lower(i.n_tech) = t.nomad_id
    where
        i.etat_intervention not in ('annulée', 'mise en échec')
        and (a.n_planning is not null or k.n_planning is not null)
),

-- Pièces réelles posées : une ligne par (intervention, code_article), hors pseudo-articles
-- (lignes de saisie d'intervention : détartrage, « pas de pièce », préventives...).
pieces as (
    select
        ar.n_planning,
        upper(ar.code_article) as code_article,
        concat('EVS_NESPRESSO_', upper(ar.code_article)) as product_reference,
        min(ar.nom_article) as libelle,
        sum(ar.quantite_article) as quantite
    from {{ ref('int_nesp_tech__articles_dedup') }} as ar
    inner join candidats as c
        on ar.n_planning = c.n_planning
    left join {{ ref('ref_yuman__pseudo_article') }} as pa
        on lower(concat('EVS_NESPRESSO_', upper(ar.code_article))) = lower(pa.reference)
    where pa.reference is null
    group by
        ar.n_planning,
        upper(ar.code_article),
        concat('EVS_NESPRESSO_', upper(ar.code_article))
),

-- Pas d'export de stock le dimanche : pour chaque date de pose, dernier export global
-- strictement antérieur, au plus 3 jours avant. Aucun export trouvé : stock inconnu (null).
export_dates as (
    select distinct export_date
    from {{ ref('stg_yuman_evs_sftp__stock_theorique') }}
),

export_veille as (
    select
        p.date_pose,
        max(e.export_date) as stock_date
    from (select distinct date_pose from candidats) as p
    inner join export_dates as e
        on
            p.date_pose > e.export_date
            and date_sub(p.date_pose, interval 3 day) <= e.export_date
    group by p.date_pose
),

stock_dates as (
    select distinct stock_date
    from export_veille
),

-- La source contient de vrais doublons de lignes : on additionne, comme Cockpit Supply.
stock_van as (
    select
        s.export_date as stock_date,
        s.nom_du_stock,
        s.reference,
        sum(s.quantite) as quantite
    from {{ ref('stg_yuman_evs_sftp__stock_theorique') }} as s
    inner join stock_dates as sd
        on s.export_date = sd.stock_date
    where starts_with(s.reference, 'EVS_NESPRESSO_')
    group by s.export_date, s.nom_du_stock, s.reference
),

pieces_detail as (
    select
        p.n_planning,
        p.code_article,
        p.libelle,
        p.quantite,
        -- Un prix à 0 du seed est un tarif inconnu, pas une pièce gratuite
        -- (même règle que dim_technique__piece_nespresso).
        nullif(prix.article_prix_unitaire, 0) as prix_unitaire,
        nullif(prix.article_prix_unitaire, 0) * p.quantite as montant,
        -- Van ou export inconnu : null. Export trouvé sans ligne pour la pièce : 0.
        case
            when c.van_technicien is null or ev.stock_date is null then null
            else coalesce(sv.quantite, 0)
        end as stock_van_veille,
        pc.code_article is not null as is_controlee
    from pieces as p
    inner join candidats as c
        on p.n_planning = c.n_planning
    -- Même jointure que fct_technique__piece_detachee_pricing_nespresso.
    left join {{ ref('ref_nesp_tech__articles_prix') }} as prix
        on lower(p.code_article) = lower(prix.article_ref_nomad)
    left join {{ ref('ref_nesp_tech__piece_controle_stock') }} as pc
        on p.code_article = pc.code_article
    left join export_veille as ev
        on c.date_pose = ev.date_pose
    -- Égalité stricte van / référence, comme fct_supply_chain__rupture_depot_yuman.
    left join stock_van as sv
        on
            ev.stock_date = sv.stock_date
            and c.van_technicien = sv.nom_du_stock
            and p.product_reference = sv.reference
),

-- Pose sans stock : pièce contrôlée posée en quantité strictement supérieure à un stock connu.
-- Stock inconnu (null) : jamais signalé.
detail as (
    select
        c.n_planning,
        c.alerte_int,
        c.van_technicien,
        coalesce(
            logical_or(d.is_controlee and d.quantite > d.stock_van_veille),
            false
        ) as has_pose_sans_stock,
        -- array_agg rend null quand aucune pièce : on force un tableau vide.
        coalesce(
            array_agg(
                if(
                    d.code_article is null,
                    null,
                    struct(
                        d.code_article,
                        d.libelle,
                        d.quantite,
                        d.prix_unitaire,
                        d.montant,
                        d.stock_van_veille
                    )
                )
                ignore nulls
                order by d.code_article
            ),
            []
        ) as pieces
    from candidats as c
    left join pieces_detail as d
        on c.n_planning = d.n_planning
    group by c.n_planning, c.alerte_int, c.van_technicien
)

-- alerte_compile : alerte de l'intermédiaire, puis POSE SANS STOCK, séparées par '-'.
select
    n_planning,
    array_to_string(
        [
            nullif(alerte_int, ''),
            if(has_pose_sans_stock, 'POSE SANS STOCK', null)
        ],
        '-'
    ) as alerte_compile,
    van_technicien,
    pieces
from detail
where coalesce(alerte_int, '') != '' or has_pose_sans_stock
