

-- Population : interventions en alerte de consommation (alerte_compile non vide), avec une
-- pose sans stock (quantité posée d'une pièce contrôlée supérieure au stock du van la veille)
-- ou avec un stock van inconnu (van absent de l'export retenu). Les alertes viennent de
-- l'intermédiaire partagé avec le mart Power BI ; les deux autres règles vivent ici seules.
-- BigQuery recalcule une CTE à chaque lecture : les CTE lourdes (pièces, stock) ne sont lues
-- qu'une fois, les règles se calculent dans l'agrégation finale.
with alertes_int as (
    select
        n_planning,
        alerte_compile
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__alerte_conso_aguila`
    where coalesce(alerte_compile, '') != ''
),

-- Van du technicien : fiche Yuman (technicien, ou manager technicien) jointe à son stock, lue
-- au staging (règle apps : pas de mart). Un nomad_id = un van, rien ne garantit son unicité :
-- on garde la fiche active, à défaut la plus récemment mise à jour.
tech as (
    select
        lower(u.nomad_id) as nomad_id,
        s.storehouses_name
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users` as u
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses` as s
        on u.user_id = s.storehouses_id
    where
        u.nomad_id is not null
        and (
            u.user_type = 'technician'
            or (u.user_type = 'manager' and u.is_manager_as_technician)
        )
    qualify row_number() over (
        partition by lower(u.nomad_id)
        order by u.is_active desc, u.updated_at desc
    ) = 1
),

-- Interventions portant au moins une pièce contrôlée.
avec_piece_controlee as (
    select distinct ar.n_planning
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup` as ar
    inner join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__piece_controle_stock` as pc
        on upper(ar.code_article) = pc.code_article
),

-- Interventions candidates, hors annulées ou en échec, tous types confondus : celles en alerte
-- et celles qui portent une pièce contrôlée. date_pose : jour de fin d'intervention à Paris
-- (date_heure_fin est en UTC : sans fuseau, une fin entre 0 h et 2 h tomberait la veille).
candidats as (
    select
        i.n_planning,
        a.alerte_compile as alerte_int,
        date(i.date_heure_fin, 'Europe/Paris') as date_pose,
        t.storehouses_name as van_technicien
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as i
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
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup` as ar
    inner join candidats as c
        on ar.n_planning = c.n_planning
    left join `evs-datastack-prod`.`prod_reference`.`ref_yuman__pseudo_article` as pa
        on lower(concat('EVS_NESPRESSO_', upper(ar.code_article))) = lower(pa.reference)
    where pa.reference is null
    group by
        ar.n_planning,
        upper(ar.code_article),
        concat('EVS_NESPRESSO_', upper(ar.code_article))
),

-- L'export daté J est écrit vers minuit et chargé vers 5 h 30 UTC : il porte l'état du soir de
-- J-1. Stock de la veille d'une pose = dernier export daté au plus tard du jour de la pose, au
-- plus 3 jours avant (trous d'export). Aucun export trouvé : stock inconnu (null).
export_dates as (
    select distinct export_date
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique`
),

export_veille as (
    select
        p.date_pose,
        max(e.export_date) as stock_date
    from (select distinct date_pose from candidats) as p
    inner join export_dates as e
        on
            p.date_pose >= e.export_date
            and date_sub(p.date_pose, interval 3 day) <= e.export_date
    group by p.date_pose
),

stock_dates as (
    select distinct stock_date
    from export_veille
),

-- Vans présents dans l'export retenu (une ligne quelconque). Un van connu mais absent de cet
-- export est un trou de données, pas un stock à 0.
vans_export as (
    select distinct
        s.export_date as stock_date,
        s.nom_du_stock
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique` as s
    inner join stock_dates as sd
        on s.export_date = sd.stock_date
),

-- La source contient de vrais doublons de lignes : on additionne, comme Cockpit Supply.
stock_van as (
    select
        s.export_date as stock_date,
        s.nom_du_stock,
        s.reference,
        sum(s.quantite) as quantite
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique` as s
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
        -- (même règle que app_suivi_tech__technique_piece_nespresso).
        nullif(prix.article_prix_unitaire, 0) as prix_unitaire,
        nullif(prix.article_prix_unitaire, 0) * p.quantite as montant,
        -- Van ou export inconnu : null. Van absent de l'export : null (trou de données).
        -- Van présent sans ligne pour la pièce : 0.
        case
            when c.van_technicien is null or ev.stock_date is null or ve.nom_du_stock is null
                then null
            else coalesce(sv.quantite, 0)
        end as stock_van_veille,
        c.van_technicien is not null and ev.stock_date is not null and ve.nom_du_stock is null
            as is_van_absent,
        pc.code_article is not null as is_controlee
    from pieces as p
    inner join candidats as c
        on p.n_planning = c.n_planning
    -- Même jointure que fct_technique__piece_detachee_pricing_nespresso.
    left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix` as prix
        on lower(p.code_article) = lower(prix.article_ref_nomad)
    left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__piece_controle_stock` as pc
        on p.code_article = pc.code_article
    left join export_veille as ev
        on c.date_pose = ev.date_pose
    left join vans_export as ve
        on
            ev.stock_date = ve.stock_date
            and c.van_technicien = ve.nom_du_stock
    -- Égalité stricte van / référence, comme fct_supply_chain__rupture_depot_yuman.
    left join stock_van as sv
        on
            ev.stock_date = sv.stock_date
            and c.van_technicien = sv.nom_du_stock
            and p.product_reference = sv.reference
),

-- Pose sans stock : pièce contrôlée posée en quantité strictement supérieure à un stock connu.
-- Stock inconnu parce que le van est absent de l'export : STOCK VAN INCONNU. Stock inconnu
-- faute de van ou d'export dans la fenêtre : aucun signalement. Les deux causes s'excluent
-- (le van est unique par intervention, et un van absent a un stock null pour toutes ses pièces).
detail as (
    select
        c.n_planning,
        c.alerte_int,
        c.van_technicien,
        coalesce(
            logical_or(d.is_controlee and d.quantite > d.stock_van_veille),
            false
        ) as has_pose_sans_stock,
        coalesce(logical_or(d.is_controlee and d.is_van_absent), false) as has_stock_van_inconnu,
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

-- alerte_compile : alerte de l'intermédiaire, puis POSE SANS STOCK ou STOCK VAN INCONNU,
-- séparées par '-'.
select
    n_planning,
    array_to_string(
        [
            nullif(alerte_int, ''),
            if(has_pose_sans_stock, 'POSE SANS STOCK', null),
            if(has_stock_van_inconnu, 'STOCK VAN INCONNU', null)
        ],
        '-'
    ) as alerte_compile,
    van_technicien,
    pieces
from detail
where coalesce(alerte_int, '') != '' or has_pose_sans_stock or has_stock_van_inconnu