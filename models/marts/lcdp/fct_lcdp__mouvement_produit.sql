{{
    config(
        materialized='table',
        partition_by={'field': 'mouvement_date', 'data_type': 'date'},
        cluster_by=['device_id', 'product_id']
    )
}}

-- Mouvements de stock produit en machine (LCDP), au grain produit × machine × jour.
-- Réponse au besoin BI « analyse produits » : quantités chargées, retirées et
-- constatées invendues, avec leur valorisation, lisibles par référence produit.
--
-- DEUX FLUX DISTINCTS, JAMAIS CUMULÉS EN UNE SEULE MESURE :
--   - chargement (task_type 13) → qty_chargee (entrée en machine) et qty_retiree
--     (produit ressorti pendant le passage, saisi en quantité négative — le sens
--     vient du SIGNE, cf. movement_type dans int_oracle_lcdp__chargement_tasks) ;
--   - invendus (task_type 11) → qty_invendus, constat dédié (péremption / casse).
-- 266 couples produit × machine × jour portent un chargement ET un retrait le même
-- jour : les sommer masquerait les deux mouvements. D'où trois colonnes séparées,
-- toutes additives, et aucun ratio calculé ici (la règle « un retrait est-il une
-- perte ? » relève du métier, elle se pose en BI).
--
-- PÉRIMÈTRE : DA FROID en full télémétrie Nayax — même parc que
-- fct_lcdp__chargement_sortie, pour que les deux rapports BI réconcilient.
-- Le filtre « produits vendables » de chargement_sortie n'est PAS repris : sur ce
-- parc il ne retirerait que 13 lignes, et une analyse produits a vocation à voir
-- tout ce qui est chargé.
--
-- VALORISATION : quantité × prix d'achat COURANT de la fiche produit, méthode
-- identique à fct_supply_chain__flux_neshu. Conséquence assumée : une valorisation
-- passée se recalcule si un tarif évolue — ce n'est pas un chiffre comptable figé.
-- Ce sont des prix d'ACHAT : aucune valorisation au prix de vente n'est possible
-- ici (les tâches de chargement et d'invendus ne portent pas de prix de vente).

with devices_perimeter as (
    select device_id
    from {{ ref('dim_lcdp__device') }}
    where
        audit_type = '1- AUDIT TELEMETRIE (NAYAX)'
        and device_category = 'DA FROID'
        and currency_mode = 'SANS MONNAIE'
),

chargement_daily as (
    select
        date(c.task_start_date) as mouvement_date,
        c.device_id,
        c.product_id,
        max(c.company_id) as company_id,
        sum(case when c.movement_type = 'LOADING' then c.load_quantity else 0 end)
            as qty_chargee,
        sum(case when c.movement_type = 'REMOVING' then -c.load_quantity else 0 end)
            as qty_retiree,
        sum(case when c.movement_type = 'LOADING' then c.load_valuation else 0 end)
            as valeur_chargee_eur,
        sum(case when c.movement_type = 'REMOVING' then -c.load_valuation else 0 end)
            as valeur_retiree_eur,
        count(distinct c.task_id) as nb_taches_chargement,
        max(c.updated_at) as updated_at
    from {{ ref('int_oracle_lcdp__chargement_tasks') }} as c
    inner join devices_perimeter as dp on c.device_id = dp.device_id
    where date(c.task_start_date) >= date('2025-01-01')
    group by 1, 2, 3
),

invendus_daily as (
    select
        date(i.task_start_date) as mouvement_date,
        i.device_id,
        i.product_id,
        max(i.company_id) as company_id,
        sum(i.quantity) as qty_invendus,
        sum(i.valuation) as valeur_invendus_eur,
        count(distinct i.task_id) as nb_constats_invendus,
        max(i.updated_at) as updated_at
    from {{ ref('int_oracle_lcdp__invendus_tasks') }} as i
    inner join devices_perimeter as dp on i.device_id = dp.device_id
    where date(i.task_start_date) >= date('2025-01-01')
    group by 1, 2, 3
)

-- FULL OUTER JOIN obligatoire : 37 % des constats d'invendus n'ont pas de
-- chargement sur le même produit × machine × jour. Un LEFT JOIN sur le
-- chargement en perdrait plus d'un tiers.
select
    coalesce(c.mouvement_date, i.mouvement_date) as mouvement_date,
    coalesce(c.device_id, i.device_id) as device_id,
    coalesce(c.product_id, i.product_id) as product_id,
    coalesce(c.company_id, i.company_id) as company_id,

    coalesce(c.qty_chargee, 0) as qty_chargee,
    coalesce(c.qty_retiree, 0) as qty_retiree,
    coalesce(i.qty_invendus, 0) as qty_invendus,

    coalesce(c.valeur_chargee_eur, 0) as valeur_chargee_eur,
    coalesce(c.valeur_retiree_eur, 0) as valeur_retiree_eur,
    coalesce(i.valeur_invendus_eur, 0) as valeur_invendus_eur,

    coalesce(c.nb_taches_chargement, 0) as nb_taches_chargement,
    coalesce(i.nb_constats_invendus, 0) as nb_constats_invendus,

    greatest(
        coalesce(c.updated_at, timestamp('1970-01-01')),
        coalesce(i.updated_at, timestamp('1970-01-01'))
    ) as updated_at

from chargement_daily as c
full outer join invendus_daily as i
    on
        c.mouvement_date = i.mouvement_date
        and c.device_id = i.device_id
        and c.product_id = i.product_id
