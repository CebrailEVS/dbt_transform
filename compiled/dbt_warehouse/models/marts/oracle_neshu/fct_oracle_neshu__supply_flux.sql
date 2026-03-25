

-- PARAMETRES




-- CTE inventaire_base : inventaires valides + ressources actives
with inventaire_base as (
    select
        i.source_code,
        i.product_code,
        i.valuation,
        date(i.task_start_date) as task_date,
        date_trunc(date(i.task_start_date), month) as mois
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inventaire_tasks` as i
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__resources` as r on i.source_code = r.resources_code
    where
        i.task_status_code = 'VALIDE'
        and date(i.task_start_date) >= '2024-12-01'
        and (r.is_active = true or r.is_active is null)
),

-- CTE inventaire_dates : calcul date de référence
inventaire_dates as (
    select
        b.source_code,
        b.mois,
        max(b.task_date) as last_date_month,
        array_agg(
            case
                when
                    date_trunc(b2.task_date, month) = date_add(b.mois, interval 1 month)
                    and extract(day from b2.task_date) <= 5
                    then b2.task_date
            end ignore nulls
            order by b2.task_date
        )[safe_offset(0)] as first_next
    from inventaire_base as b
    left join
        inventaire_base as b2
        on b.source_code = b2.source_code and b2.task_date between b.mois and date_add(b.mois, interval 40 day)
    group by b.source_code, b.mois
),

-- CTE inventaire_reference : fallback
inventaire_reference as (
    select
        source_code,
        mois,
        coalesce(last_date_month, first_next) as ref_date
    from inventaire_dates
),

-- CTE inventaire_reel : inventaire retenu
inventaire_reel as (
    select
        r.mois,
        i.source_code,
        i.product_code,
        i.valuation
    from inventaire_reference as r
    inner join inventaire_base as i on r.source_code = i.source_code and r.ref_date = i.task_date
),

-- CTE inventaire_sources_mois : sources avec inventaire réel
inventaire_sources_mois as (
    select distinct
        source_code,
        mois
    from inventaire_reel
),

-- CTE stock_theorique : fallback
stock_theorique as (
    select
        date_trunc(date(st.date_system), month) as mois,
        st.resources_code as source_code,
        st.product_code,
        p.purchase_unit_price * st.stock_at_date as valuation
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu_gcs__stock_theorique` as st
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product` as p on st.product_code = p.product_code
    left join
        inventaire_sources_mois as ir
        on st.resources_code = ir.source_code and ir.mois = date_trunc(date(st.date_system), month)
    left join `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__resources` as rr on st.resources_code = rr.resources_code
    where ir.source_code is null and rr.is_active = true
    qualify
        row_number()
            over (
                partition by st.resources_code, st.product_code, date_trunc(date(st.date_system), month)
                order by st.date_system desc
            )
        = 1
),

-- CTE flux_unifies : normalisation flux
flux_unifies as (
    select
        mois as flux_date,
        valuation as valeur,
        null as valeur_theorique,
        case
            when source_code = 'DEPOTRUNGIS' then 'STOCK_DEPOT_RUNGIS'
            when source_code = 'DEPOTLYON' then 'STOCK_DEPOT_LYON'
            when source_code = 'DEPOTMARSEILLE' then 'STOCK_DEPOT_MARSEILLE'
            when source_code in ('ANIMLYON', 'ANIMRUNGIS') then 'STOCK_ANIM'
            when source_code in ('PREPALYON', 'PREPARUNGIS') then 'STOCK_PREPA'
            else 'STOCK_VEHICULE'
        end as flux_type
    from inventaire_reel
    union all
    select
        mois as flux_date,
        null as valeur,
        valuation as valeur_theorique,
        case
            when source_code in ('ANIMLYON', 'ANIMRUNGIS') then 'STOCK_ANIM_THEORIQUE'
            when source_code in ('PREPALYON', 'PREPARUNGIS') then 'STOCK_PREPA_THEORIQUE'
            else 'STOCK_THEORIQUE'
        end as flux_type
    from stock_theorique
    union all
    select
        date(task_start_date) as flux_date,
        valuation as valeur,
        null as valeur_theorique,
        case
            when destination_code in ('ANIMLYON', 'ANIMRUNGIS') and source_code <> 'V50' then 'LIVRAISON_ANIM'
            when destination_code in ('DEPOTPERIMES', 'DEPOTREBUS') then 'LIVRAISON_PERIME'
            when
                destination_code like 'V%' or destination_code in ('RATP', 'ARKEMA', 'STRASBOURG')
                then 'LIVRAISON_VEHICULE'
            when destination_code in ('PREPARUNGIS', 'PREPALYON') then 'LIVRAISON_PREPA'
            else 'LIVRAISON_INTERNE_GLOBAL'
        end as flux_type
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_interne_tasks`
    where
        task_status_code in ('FAIT', 'VALIDE')
        and date(task_start_date) between '2025-01-01' and '9999-12-31'
    union all
    select
        date(task_start_date) as flux_date,
        valuation as valeur,
        null as valeur_theorique,
        'RECEPTION_FOURNISSEUR' as flux_type
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__reception_tasks`
    where
        task_status_code in ('FAIT', 'VALIDE')
        and destination_code not in (
            'DEPOTATELIERBORDEAUX', 'DEPOTATELIERLYON', 'DEPOTATELIERMARSEILLE', 'DEPOTATELIERRUNGIS'
        )
        and date(task_start_date) between '2025-01-01' and '9999-12-31'
    union all
    select
        date(task_start_date) as flux_date,
        valuation as valeur,
        null as valeur_theorique,
        'LIVRAISON_CLIENT' as flux_type
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_tasks`
    where
        task_status_code in ('VALIDE', 'FAIT')
        and date(task_start_date) between '2025-01-01' and '9999-12-31'
    union all
    select
        date(task_start_date) as flux_date,
        load_valuation as valeur,
        null as valeur_theorique,
        'CHARGEMENT_MACHINE' as flux_type
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks`
    where date(task_start_date) between '2025-01-01' and '9999-12-31'
),

-- CTE agg : agrégation mensuelle
agg as (
    select
        date_trunc(flux_date, month) as mois,
        sum(if(flux_type = 'STOCK_DEPOT_RUNGIS', valeur, 0)) as stock_depot_rungis,
        sum(if(flux_type = 'STOCK_DEPOT_LYON', valeur, 0)) as stock_depot_lyon,
        sum(if(flux_type = 'STOCK_DEPOT_MARSEILLE', valeur, 0)) as stock_depot_marseille,
        sum(if(flux_type in ('STOCK_DEPOT_RUNGIS', 'STOCK_DEPOT_LYON', 'STOCK_DEPOT_MARSEILLE'), valeur, 0))
            as stock_depot,
        sum(if(flux_type = 'STOCK_VEHICULE', valeur, 0)) as stock_vehicule,
        sum(if(flux_type = 'STOCK_ANIM_THEORIQUE', valeur_theorique, 0)) as stocks_theoriques_anim,
        sum(if(flux_type = 'STOCK_PREPA_THEORIQUE', valeur_theorique, 0)) as stocks_theoriques_prepa,
        sum(if(flux_type = 'STOCK_THEORIQUE', valeur_theorique, 0)) as stocks_theoriques_autre,
        sum(if(flux_type = 'RECEPTION_FOURNISSEUR', valeur, 0)) as reception_fournisseur,
        sum(if(flux_type = 'LIVRAISON_CLIENT', valeur, 0)) as livraison_client,
        sum(if(flux_type = 'LIVRAISON_VEHICULE', valeur, 0)) as livraison_vehicule,
        sum(if(flux_type = 'LIVRAISON_ANIM', valeur, 0)) as livraison_anim,
        sum(if(flux_type = 'LIVRAISON_PERIME', valeur, 0)) as livraison_perime,
        sum(if(flux_type = 'CHARGEMENT_MACHINE', valeur, 0)) as chargement_machine,
        sum(if(flux_type = 'LIVRAISON_PREPA', valeur, 0)) as livraison_prepa,
        sum(if(flux_type = 'LIVRAISON_INTERNE_GLOBAL', valeur, 0)) as livraison_interne_autre
    from flux_unifies
    group by mois
)

-- RESULTAT FINAL
select
    mois as mois_date,
    extract(year from mois) as annee,
    extract(month from mois) as mois,
    stock_depot_rungis,
    stock_depot_lyon,
    stock_depot_marseille,
    stock_depot,
    stock_vehicule,
    stock_depot + stock_vehicule as stock_total,
    stocks_theoriques_anim,
    stocks_theoriques_prepa,
    stocks_theoriques_autre,
    reception_fournisseur,
    livraison_client,
    livraison_vehicule,
    livraison_anim,
    livraison_perime,
    chargement_machine,
    livraison_prepa,
    livraison_interne_autre,
    current_timestamp() as dbt_updated_at,
    'eba019cb-b65a-45ed-9181-967a1836546c' as dbt_invocation_id
from agg