

with devices_perimeter as (
    select device_id
    from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`
    where
        audit_type = '1- AUDIT TELEMETRIE (NAYAX)'
        and device_category = 'DA FROID'
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
        sum(c.load_valuation) as cout_achat_chargement_eur,
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
        count(*) as nb_ventes,
        sum(t.sale_amount_net_eur) as ca_ht_sortie_eur,
        sum(t.sale_amount_net_tax_eur) as ca_ttc_sortie_eur
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
        coalesce(c.cout_achat_chargement_eur, 0) as cout_achat_chargement_eur,
        coalesce(c.nb_chargements, 0) as nb_chargements,
        coalesce(t.nb_ventes, 0) as nb_ventes,
        coalesce(t.ca_ht_sortie_eur, 0) as ca_ht_sortie_eur,
        coalesce(t.ca_ttc_sortie_eur, 0) as ca_ttc_sortie_eur
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
        cout_achat_chargement_eur,
        nb_chargements,
        nb_ventes,
        ca_ht_sortie_eur,
        ca_ttc_sortie_eur,

        sum(qty_vendable_chargee) over wk4 as qty_vendable_chargee_4wk,
        sum(cout_achat_chargement_eur) over wk4 as cout_achat_chargement_4wk_eur,
        sum(nb_ventes) over wk4 as nb_ventes_4wk,
        sum(ca_ht_sortie_eur) over wk4 as ca_ht_sortie_4wk_eur,
        sum(ca_ttc_sortie_eur) over wk4 as ca_ttc_sortie_4wk_eur

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

    qty_vendable_chargee,
    cout_achat_chargement_eur,
    nb_chargements,
    nb_ventes,
    ca_ht_sortie_eur,
    ca_ttc_sortie_eur,
    safe_divide(nb_ventes, qty_vendable_chargee) as taux_ecoulement_volume_hebdo,
    safe_divide(ca_ht_sortie_eur, cout_achat_chargement_eur) as taux_marge_brute_apparent_hebdo,
    ca_ht_sortie_eur - cout_achat_chargement_eur as marge_brute_apparente_hebdo_eur,

    qty_vendable_chargee_4wk,
    cout_achat_chargement_4wk_eur,
    nb_ventes_4wk,
    ca_ht_sortie_4wk_eur,
    ca_ttc_sortie_4wk_eur,
    safe_divide(nb_ventes_4wk, qty_vendable_chargee_4wk) as taux_ecoulement_volume_4wk,
    safe_divide(ca_ht_sortie_4wk_eur, cout_achat_chargement_4wk_eur) as taux_marge_brute_apparent_4wk,
    ca_ht_sortie_4wk_eur - cout_achat_chargement_4wk_eur as marge_brute_apparente_4wk_eur

from with_rolling