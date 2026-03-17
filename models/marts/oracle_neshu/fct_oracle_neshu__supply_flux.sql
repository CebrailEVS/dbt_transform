{{ config(
materialized='table',
partition_by={
"field":"mois_date",
"data_type":"date"
}
) }}

-- =========================================================
-- OBJECTIF
-- Construire une table de faits mensuelle regroupant
-- l'ensemble des flux supply Neshu :
-- - Stocks réels (inventaires)
-- - Stocks théoriques
-- - Réceptions fournisseurs
-- - Livraisons internes
-- - Livraisons clients
-- - Chargements machines
-- =========================================================

with inventaire_base as (
-- Extraction des inventaires validés
-- On normalise les dates au format jour + mois
    select
        source_code,
        product_code,
        valuation,
        DATE(task_start_date) as task_date,
        DATE(DATE_TRUNC(task_start_date, month)) as mois
    from {{ ref('int_oracle_neshu__inventaire_tasks') }}
    where
        task_status_code = 'VALIDE'
        and task_start_date >= '2024-12-01'
),

inventaire_dates as (
-- Pour chaque source et chaque mois :
-- - dernière date d'inventaire du mois
-- - première date <= 5 du mois suivant (fallback)
    select
        source_code,
        mois,
        MAX(task_date) as last_date_month,
        MIN(IF(EXTRACT(day from task_date) <= 5, task_date, null)) as first_next
    from inventaire_base
    group by source_code, mois
),

inventaire_reference as (
-- Choix de la date de référence d'inventaire
-- priorité à la dernière date du mois
    select
        source_code,
        mois,
        COALESCE(last_date_month, first_next) as ref_date
    from inventaire_dates
),

inventaire_reel as (
-- Sélection des lignes d'inventaire correspondant
-- à la date de référence retenue
    select
        r.mois,
        i.source_code,
        i.product_code,
        i.valuation
    from inventaire_reference as r
    inner join inventaire_base as i
        on
            r.source_code = i.source_code
            and r.ref_date = i.task_date
),

inventaire_sources_mois as (
-- Liste des sources ayant un inventaire réel sur le mois
-- Sert à exclure ces cas du stock théorique
    select distinct
        source_code,
        mois
    from inventaire_reel
),

stock_theorique as (
-- Stock théorique utilisé uniquement
-- lorsque l'inventaire réel n'existe pas
    select
        DATE(DATE_TRUNC(st.date_system, month)) as mois,
        st.resources_code as source_code,
        st.product_code,
        p.purchase_unit_price * st.stock_at_date as valuation
    from {{ ref('stg_oracle_neshu_gcs__stock_theorique') }} as st
    left join {{ ref('dim_oracle_neshu__product') }} as p
        on st.product_code = p.product_code
    left join inventaire_sources_mois as ir
        on
            st.resources_code = ir.source_code
            and ir.mois = DATE(DATE_TRUNC(st.date_system, month))
    where ir.source_code is null
    -- on conserve la dernière photo de stock du mois
    qualify ROW_NUMBER() over (
        partition by
            st.resources_code,
            st.product_code,
            DATE_TRUNC(st.date_system, month)
        order by st.date_system desc
    ) = 1
),

flux_unifies as (
-- =========================================================
-- UNION de tous les flux supply
-- Chaque flux est typé via flux_type
-- =========================================================

-- STOCK REEL (inventaires)
    select
        mois as date,
        valuation as valeur,
        null as valeur_theorique,
        case
            when source_code in ('DEPOTRUNGIS', 'DEPOTLYON', 'DEPOTMARSEILLE')
                then 'STOCK_DEPOT'
            else 'STOCK_VEHICULE'
        end as flux_type
    from inventaire_reel
    where
        source_code like 'V%'
        or source_code in ('STRASBOURG', 'ARKEMA', 'RATP', 'DEPOTRUNGIS', 'DEPOTLYON', 'DEPOTMARSEILLE')

    union all

    -- STOCK THEORIQUE (fallback)
    select
        mois as date,
        null as valeur,
        valuation as valeur_theorique,
        'STOCK_THEORIQUE'
    from stock_theorique
    where
        source_code like 'V%'
        or source_code in ('STRASBOURG', 'ARKEMA', 'RATP')

    union all

    -- LIVRAISONS INTERNES
    select
        DATE(task_start_date),
        valuation,
        null,
        case
            when
                destination_code in ('ANIMLYON', 'ANIMRUNGIS')
                and source_code <> 'V50'
                then 'LIVRAISON_ANIM'
            when destination_code in ('DEPOTPERIMES', 'DEPOTREBUS')
                then 'LIVRAISON_PERIME'
            when
                destination_code like 'V%'
                or destination_code in ('RATP', 'ARKEMA', 'STRASBOURG')
                then 'LIVRAISON_VEHICULE'
            when destination_code in ('PREPARUNGIS', 'PREPALYON')
                then 'LIVRAISON_PREPA'
            else 'LIVRAISON_INTERNE_GLOBAL'
        end
    from {{ ref('int_oracle_neshu__livraison_interne_tasks') }}
    where
        task_status_code in ('FAIT', 'VALIDE')
        and DATE(task_start_date) between '2025-01-01' and '2026-12-31'

    union all

    -- RECEPTION FOURNISSEUR
    select
        DATE(task_start_date),
        valuation,
        null,
        'RECEPTION_FOURNISSEUR'
    from {{ ref('int_oracle_neshu__reception_tasks') }}
    where
        task_status_code in ('FAIT', 'VALIDE')
        and destination_code not in (
            'DEPOTATELIERBORDEAUX',
            'DEPOTATELIERLYON',
            'DEPOTATELIERMARSEILLE',
            'DEPOTATELIERRUNGIS'
        )
        and DATE(task_start_date) between '2025-01-01' and '2026-12-31'

    union all

    -- LIVRAISON CLIENT
    select
        DATE(task_start_date),
        valuation,
        null,
        'LIVRAISON_CLIENT'
    from {{ ref('int_oracle_neshu__livraison_tasks') }}
    where
        task_status_code in ('VALIDE', 'FAIT')
        and DATE(task_start_date) between '2025-01-01' and '2026-12-31'

    union all

    -- CHARGEMENT MACHINE
    select
        DATE(task_start_date),
        load_valuation,
        null,
        'CHARGEMENT_MACHINE'
    from {{ ref('int_oracle_neshu__chargement_tasks') }}
    where DATE(task_start_date) between '2025-01-01' and '2026-12-31'
),

agg as (
-- Agrégation mensuelle de tous les flux
    select
        DATE_TRUNC(date, month) as mois,
        SUM(IF(flux_type = 'STOCK_DEPOT', valeur, 0)) as stock_depot,
        SUM(IF(flux_type = 'STOCK_VEHICULE', valeur, 0)) as stock_vehicule,
        SUM(valeur_theorique) as stocks_theoriques,
        SUM(IF(flux_type = 'RECEPTION_FOURNISSEUR', valeur, 0)) as reception_fournisseur,
        SUM(IF(flux_type = 'LIVRAISON_CLIENT', valeur, 0)) as livraison_client,
        SUM(IF(flux_type = 'LIVRAISON_VEHICULE', valeur, 0)) as livraison_vehicule,
        SUM(IF(flux_type = 'LIVRAISON_ANIM', valeur, 0)) as livraison_anim,
        SUM(IF(flux_type = 'LIVRAISON_PERIME', valeur, 0)) as livraison_perime,
        SUM(IF(flux_type = 'CHARGEMENT_MACHINE', valeur, 0)) as chargement_machine,
        SUM(IF(flux_type = 'LIVRAISON_PREPA', valeur, 0)) as livraison_prepa,
        SUM(IF(flux_type = 'LIVRAISON_INTERNE_GLOBAL', valeur, 0)) as livraison_interne_autre
    from flux_unifies
    group by mois
)

-- =========================================================
-- RESULTAT FINAL
-- Table de faits mensuelle supply
-- =========================================================
select
    mois as mois_date,
    EXTRACT(year from mois) as annee,
    EXTRACT(month from mois) as mois,

    stock_depot,
    stock_vehicule,
    stock_depot + stock_vehicule as stock_total,

    stocks_theoriques,

    reception_fournisseur,
    livraison_client,
    livraison_vehicule,
    livraison_anim,
    livraison_perime,
    chargement_machine,
    livraison_prepa,
    livraison_interne_autre

from agg
where EXTRACT(year from mois) >= 2025
