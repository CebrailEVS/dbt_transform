

-- Taux d'écoulement (VOLUME) des DA FROID full Nayax, par device × semaine ISO.
-- Modèle VOLUME PUR : entrées (qty vendable chargée) vs sorties (nb ventes Nayax).
-- Aucune mesure monétaire ici : le CA est porté par fct_lcdp__ca_mensuel (tout le
-- parc + cash). Périmètre limité au full Nayax car sur les machines à monnayeur
-- une partie des ventes part en espèces (invisible côté Nayax) → le volume serait faux.

with devices_perimeter as (
    select device_id
    from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`
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
    from `evs-datastack-prod`.`prod_reference`.`ref_oracle_lcdp__product_group_vendable`
    where is_vendable
),

chargement_weekly as (
    select
        c.device_id,
        date_trunc(date(c.task_start_date), week (monday)) as week_start_date,
        sum(c.load_quantity) as qty_vendable_chargee,
        count(distinct c.task_id) as nb_chargements
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__chargement_tasks` as c
    inner join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__product` as p
        on c.product_id = p.product_id
    inner join vendable_groups as v
        on trim(p.product_group) = v.product_group
    where
        c.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(c.task_start_date) >= date('2025-01-01')
        and c.load_type_code = 'LOADING'
    group by 1, 2
),

telemetry_weekly as (
    select
        t.device_id,
        date_trunc(date(t.task_start_date), week (monday)) as week_start_date,
        count(*) as nb_ventes
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__telemetry_tasks` as t
    where
        t.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(t.task_start_date) >= date('2025-01-01')
    group by 1, 2
),

joined as (
    select
        coalesce(c.device_id, t.device_id) as device_id,
        coalesce(c.week_start_date, t.week_start_date) as week_start_date,
        coalesce(c.qty_vendable_chargee, 0) as qty_vendable_chargee,
        coalesce(c.nb_chargements, 0) as nb_chargements,
        coalesce(t.nb_ventes, 0) as nb_ventes
    from chargement_weekly as c
    full outer join telemetry_weekly as t
        on
            c.device_id = t.device_id
            and c.week_start_date = t.week_start_date
),

with_rolling as (
    select
        device_id,
        week_start_date,
        qty_vendable_chargee,
        nb_chargements,
        nb_ventes,

        sum(qty_vendable_chargee) over wk4 as qty_vendable_chargee_4wk,
        sum(nb_ventes) over wk4 as nb_ventes_4wk

    from joined
    window wk4 as (
        partition by device_id
        order by unix_date(week_start_date)
        range between 21 preceding and current row
    )
)

select
    device_id,
    week_start_date,

    -- Briques additives (entrées)
    qty_vendable_chargee,
    nb_chargements,

    -- Briques additives (sorties)
    nb_ventes,

    -- Ratio volume (non additif — fourni pour confort, recalcul possible en BI)
    safe_divide(nb_ventes, qty_vendable_chargee) as taux_ecoulement_volume_hebdo,

    -- Briques additives rolling 4 semaines
    qty_vendable_chargee_4wk,
    nb_ventes_4wk,
    safe_divide(nb_ventes_4wk, qty_vendable_chargee_4wk) as taux_ecoulement_volume_4wk

from with_rolling