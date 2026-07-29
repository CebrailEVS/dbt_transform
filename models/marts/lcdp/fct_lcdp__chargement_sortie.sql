{{
    config(
        materialized='table',
        partition_by={'field': 'week_start_date', 'data_type': 'date'},
        cluster_by=['device_id', 'resources_roadman_id']
    )
}}

-- Taux d'écoulement (VOLUME) des DA FROID full Nayax, par device × semaine ISO.
-- Modèle VOLUME PUR : entrées (qty chargée / retirée) vs sorties (nb ventes Nayax).
-- Aucune mesure monétaire ici : le CA est porté par fct_lcdp__ca_mensuel (tout le
-- parc + cash). Périmètre limité au full Nayax car sur les machines à monnayeur
-- une partie des ventes part en espèces (invisible côté Nayax) → le volume serait faux.
--
-- PÉRIMÈTRE PRODUIT (invariant du modèle) : toutes les mesures de quantité sont
-- restreintes aux groupes de produits VENDABLES (ref_oracle_lcdp__product_group_vendable,
-- is_vendable = true : SNACK / BOISSON FROIDE / PRODUIT FRAIS). Le filtre vendable
-- s'applique à tout le modèle → il n'est PAS répété dans le nom des colonnes.
--
-- COMMENSURABILITÉ : le taux d'écoulement = nb_ventes / qty_chargee n'a de sens que
-- parce que 1 event télémétrie Nayax = 1 unité vendue (telemetry_quantity toujours = 1
-- en amont). nb_ventes est un count(*) d'events (entier), comparé à une quantité
-- déclarée chargée (somme en unités de base) : deux systèmes de mesure distincts.
--
-- ROADMAN DE LA SEMAINE : la ligne porte le roadman qui a réalisé le plus de
-- tâches de chargement sur la machine dans la semaine (demande BI 2026-07-28 :
-- monitorer les flux de marchandises au niveau roadman). Calculé sur le MÊME
-- périmètre de tâches que qty_chargee / qty_retiree (produits vendables), donc
-- la colonne qualifie exactement l'activité de chargement mesurée sur la ligne.
-- NULL sur les semaines sans chargement (~17 % des lignes : machine qui a vendu
-- sans être rechargée) — c'est attendu, il n'y a personne à nommer.
-- Il s'agit du roadman OBSERVÉ, à ne pas confondre avec le roadman AFFECTÉ à la
-- machine dans l'ERP (contact_has_device, exposé sur dim_lcdp__device) : sur les
-- 90 derniers jours, les deux coïncident sur 82,6 % des lignes comparables.
--
-- Mouvements de stock classés par movement_type (signe de la quantité, cf.
-- int_oracle_lcdp__chargement_tasks) : LOADING = chargé, REMOVING = retiré
-- (produit sorti de la machine, ex. péremption).
-- Les invendus (constat dédié, task_type 11, cf. int_oracle_lcdp__invendus_tasks)
-- sont un second canal de retrait de stock vendable, distinct du REMOVING (pas de
-- double comptage). Le taux d'écoulement NET déduit LES DEUX du dénominateur :
-- ventes / (chargé − retiré − invendus) → part vendue du stock réellement resté
-- disponible. qty_invendus reste exposé à part pour le suivi de la casse/pertes.

with devices_perimeter as (
    select device_id
    from {{ ref('dim_lcdp__device') }}
    where
        audit_type = '1- AUDIT TELEMETRIE (NAYAX)'
        and device_category = 'DA FROID'
        -- Full Nayax uniquement : sur les machines à monnayeur, une partie des
        -- ventes est encaissée en espèces (invisible côté Nayax) → le taux
        -- d'écoulement volume y serait faux. Leur suivi CA passe par
        -- fct_lcdp__ca_mensuel.
        and currency_mode = 'SANS MONNAIE'
),

vendable_groups as (
    select trim(product_group) as product_group
    from {{ ref('ref_oracle_lcdp__product_group_vendable') }}
    where is_vendable
),

-- Tâches de chargement du périmètre (machines full Nayax × produits vendables).
-- Isolées dans leur propre CTE pour que les quantités hebdo ET le roadman de la
-- semaine soient calculés sur exactement le même jeu de tâches.
chargement_tasks_scoped as (
    select
        c.device_id,
        date_trunc(date(c.task_start_date), week (monday)) as week_start_date,
        c.task_id,
        c.roadman_id,
        c.roadman_code,
        c.movement_type,
        c.load_quantity
    from {{ ref('int_oracle_lcdp__chargement_tasks') }} as c
    inner join {{ ref('dim_lcdp__product') }} as p
        on c.product_id = p.product_id
    inner join vendable_groups as v
        on trim(p.product_group) = v.product_group
    where
        c.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(c.task_start_date) >= date('2025-01-01')
),

chargement_weekly as (
    select
        device_id,
        week_start_date,
        sum(case when movement_type = 'LOADING' then load_quantity else 0 end)
            as qty_chargee,
        sum(case when movement_type = 'REMOVING' then -load_quantity else 0 end)
            as qty_retiree
    from chargement_tasks_scoped
    group by 1, 2
),

-- Roadman principal de la semaine, par machine.
-- Départage déterministe (5,6 % des device×semaine sont à égalité sur le nombre
-- de tâches) : nb de chargements, puis volume chargé, puis identifiant le plus
-- petit. Sans règle stable, la valeur changerait d'un rebuild à l'autre.
chargement_roadman_counts as (
    select
        device_id,
        week_start_date,
        roadman_id,
        roadman_code,
        count(distinct task_id) as nb_chargements,
        sum(case when movement_type = 'LOADING' then load_quantity else 0 end)
            as qty_chargee_roadman
    from chargement_tasks_scoped
    where roadman_id is not null
    group by 1, 2, 3, 4
),

chargement_roadman_weekly as (
    select
        device_id,
        week_start_date,
        roadman_id,
        roadman_code
    from chargement_roadman_counts
    qualify
        row_number() over (
            partition by device_id, week_start_date
            order by nb_chargements desc, qty_chargee_roadman desc, roadman_id asc
        ) = 1
),

telemetry_weekly as (
    select
        t.device_id,
        date_trunc(date(t.task_start_date), week (monday)) as week_start_date,
        count(*) as nb_ventes
    from {{ ref('int_oracle_lcdp__telemetry_tasks') }} as t
    where
        t.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(t.task_start_date) >= date('2025-01-01')
    group by 1, 2
),

-- Invendus (task_type 11) : produits retirés car invendus (péremption / casse).
-- Distinct de qty_retiree (retraits issus du chargement) : événement de constat
-- dédié, pas de double comptage. Même filtre vendable que le chargement.
invendus_weekly as (
    select
        i.device_id,
        date_trunc(date(i.task_start_date), week (monday)) as week_start_date,
        sum(i.quantity) as qty_invendus
    from {{ ref('int_oracle_lcdp__invendus_tasks') }} as i
    inner join {{ ref('dim_lcdp__product') }} as p
        on i.product_id = p.product_id
    inner join vendable_groups as v
        on trim(p.product_group) = v.product_group
    where
        i.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(i.task_start_date) >= date('2025-01-01')
    group by 1, 2
),

-- Contexte de réassort : nombre de passages approvisionneurs RÉALISÉS dans la semaine
-- (task_type 32, is_done = FAIT/ENCOURS, cf. int_oracle_lcdp__appro_tasks_enriched).
-- Simple contexte (combien de fois la machine a été servie) → rattaché en LEFT JOIN,
-- ne crée pas de ligne : un device×semaine sans flux de stock n'est pas ajouté ici.
appro_weekly as (
    select
        a.device_id,
        date_trunc(date(a.task_start_date), week (monday)) as week_start_date,
        sum(a.is_done) as nb_passages_appro
    from {{ ref('int_oracle_lcdp__appro_tasks_enriched') }} as a
    where
        a.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(a.task_start_date) >= date('2025-01-01')
    group by 1, 2
),

joined as (
    select
        coalesce(c.device_id, t.device_id, i.device_id) as device_id,
        coalesce(c.week_start_date, t.week_start_date, i.week_start_date) as week_start_date,
        coalesce(c.qty_chargee, 0) as qty_chargee,
        coalesce(c.qty_retiree, 0) as qty_retiree,
        coalesce(t.nb_ventes, 0) as nb_ventes,
        coalesce(i.qty_invendus, 0) as qty_invendus
    from chargement_weekly as c
    full outer join telemetry_weekly as t
        on
            c.device_id = t.device_id
            and c.week_start_date = t.week_start_date
    full outer join invendus_weekly as i
        on
            coalesce(c.device_id, t.device_id) = i.device_id
            and coalesce(c.week_start_date, t.week_start_date) = i.week_start_date
),

with_rolling as (
    select
        device_id,
        week_start_date,
        qty_chargee,
        qty_retiree,
        nb_ventes,
        qty_invendus,

        sum(qty_chargee) over wk4 as qty_chargee_4wk,
        sum(qty_retiree) over wk4 as qty_retiree_4wk,
        sum(nb_ventes) over wk4 as nb_ventes_4wk,
        sum(qty_invendus) over wk4 as qty_invendus_4wk

    from joined
    window wk4 as (
        partition by device_id
        order by unix_date(week_start_date)
        range between 21 preceding and current row
    )
)

select
    wr.device_id,
    wr.week_start_date,

    -- Roadman OBSERVÉ : celui qui a réalisé le plus de chargements sur la machine
    -- dans la semaine. NULL si aucun chargement (semaine vendue sans réassort).
    rm.roadman_id as resources_roadman_id,
    rm.roadman_code,
    r.resources_name as roadman_name,

    -- Briques additives (entrées)
    wr.qty_chargee,
    wr.qty_retiree,

    -- Briques additives (sorties)
    wr.nb_ventes,

    -- Invendus (constat task_type 11) : produits retirés car invendus (péremption / casse).
    -- Additif. Déduit du dénominateur du taux NET (même nature que le retiré).
    wr.qty_invendus,

    -- Contexte de réassort : nombre de passages approvisionneurs réalisés dans la semaine.
    -- Additif, INFORMATIF — n'entre dans aucun ratio d'écoulement. 0 si aucun passage.
    coalesce(a.nb_passages_appro, 0) as nb_passages_appro,

    -- Ratio volume (non additif — fourni pour confort, recalcul possible en BI)
    safe_divide(wr.nb_ventes, wr.qty_chargee) as taux_ecoulement_volume_hebdo,

    -- Briques additives rolling 4 semaines
    wr.qty_chargee_4wk,
    wr.qty_retiree_4wk,
    wr.nb_ventes_4wk,
    wr.qty_invendus_4wk,
    safe_divide(wr.nb_ventes_4wk, wr.qty_chargee_4wk) as taux_ecoulement_volume_4wk,

    -- Taux d'écoulement NET 4 sem. : déduit le retiré ET les invendus du dénominateur
    -- (deux retraits de stock vendable de même nature). Mesure la part vendue du stock
    -- réellement resté disponible (ni retiré, ni constaté invendu).
    -- NULL si (retiré + invendus) >= chargé (dénominateur <= 0, fenêtre de déstockage
    -- non interprétable).
    safe_divide(
        wr.nb_ventes_4wk,
        nullif(greatest(wr.qty_chargee_4wk - wr.qty_retiree_4wk - wr.qty_invendus_4wk, 0), 0)
    ) as taux_ecoulement_volume_net_4wk

from with_rolling as wr
left join chargement_roadman_weekly as rm
    on
        wr.device_id = rm.device_id
        and wr.week_start_date = rm.week_start_date
left join {{ ref('dim_lcdp__resource') }} as r
    on rm.roadman_id = r.resources_id
left join appro_weekly as a
    on
        wr.device_id = a.device_id
        and wr.week_start_date = a.week_start_date
