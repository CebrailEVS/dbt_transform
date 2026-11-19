{{ config(
    materialized='table'
) }}

-- Aucune colonne de ce mart ne doit porter un nom déjà présent dans
-- fct_technique__intervention_retraitee : l'application Suivi Tech joint les deux modèles
-- et sélectionne ses colonnes sans qualificatif de table.

-- Interventions en alerte (alerte_compile vaut '' sans alerte), avec le technicien
-- et la date de pose de leurs pièces.
with alertes as (
    select
        a.n_planning,
        a.alerte_compile,
        i.n_tech,
        -- Même convention de date que fct_technique__alerting_consommation_aguila.date_fin.
        cast(i.date_heure_fin as date) as date_pose
    from {{ ref('fct_technique__alerting_consommation_aguila') }} as a
    left join {{ ref('int_nesp_tech__interventions_dedup') }} as i
        on a.n_planning = i.n_planning
    where coalesce(a.alerte_compile, '') != ''
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

alertes_van as (
    select
        a.n_planning,
        a.alerte_compile,
        a.date_pose,
        t.storehouses_name as van_technicien
    from alertes as a
    left join tech as t
        on lower(a.n_tech) = t.nomad_id
),

articles_alerte as (
    select
        ar.n_planning,
        upper(ar.code_article) as code_article,
        ar.nom_article,
        ar.quantite_article
    from {{ ref('int_nesp_tech__articles_dedup') }} as ar
    inner join alertes as a
        on ar.n_planning = a.n_planning
),

-- Pièces réelles posées : une ligne par (intervention, code_article), hors pseudo-articles
-- (lignes de saisie d'intervention : détartrage, « pas de pièce », préventives...).
pieces as (
    select
        aa.n_planning,
        aa.code_article,
        concat('EVS_NESPRESSO_', aa.code_article) as product_reference,
        min(aa.nom_article) as libelle,
        sum(aa.quantite_article) as quantite
    from articles_alerte as aa
    left join {{ ref('ref_yuman__pseudo_article') }} as pa
        on lower(concat('EVS_NESPRESSO_', aa.code_article)) = lower(pa.reference)
    where pa.reference is null
    group by aa.n_planning, aa.code_article
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
    from (select distinct date_pose from alertes_van) as p
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
            when a.van_technicien is null or ev.stock_date is null then null
            else coalesce(sv.quantite, 0)
        end as stock_van_veille
    from pieces as p
    inner join alertes_van as a
        on p.n_planning = a.n_planning
    -- Même jointure que fct_technique__piece_detachee_pricing_nespresso.
    left join {{ ref('ref_nesp_tech__articles_prix') }} as prix
        on lower(p.code_article) = lower(prix.article_ref_nomad)
    left join export_veille as ev
        on a.date_pose = ev.date_pose
    -- Égalité stricte van / référence, comme fct_supply_chain__rupture_depot_yuman.
    left join stock_van as sv
        on
            ev.stock_date = sv.stock_date
            and a.van_technicien = sv.nom_du_stock
            and p.product_reference = sv.reference
)

select
    a.n_planning,
    a.alerte_compile,
    a.van_technicien,
    -- Une alerte sans pièce n'a aucune ligne non nulle : BigQuery stocke alors un tableau vide.
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
    ) as pieces
from alertes_van as a
left join pieces_detail as d
    on a.n_planning = d.n_planning
group by a.n_planning, a.alerte_compile, a.van_technicien
