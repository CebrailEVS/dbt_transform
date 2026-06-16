{{
    config(
        materialized='table',
        partition_by={'field': 'month_start_date', 'data_type': 'date'},
        cluster_by=['device_id']
    )
}}

with devices_perimeter as (
    select
        device_id,
        company_id,
        coalesce(currency_mode = 'AVEC MONNAIE', false) as has_monnayeur
    from {{ ref('dim_lcdp__device') }}
    where
        audit_type = '1- AUDIT TELEMETRIE (NAYAX)'
        and device_category = 'DA FROID'
),

-- CA Nayax (ventes télémétrie) agrégé au mois
nayax_monthly as (
    select
        t.device_id,
        date_trunc(date(t.task_start_date), month) as month_start_date,
        count(*) as nb_ventes_nayax,
        sum(t.sale_amount_net_eur) as ca_nayax_ht_eur,
        sum(t.sale_amount_net_tax_eur) as ca_nayax_ttc_eur
    from {{ ref('int_oracle_lcdp__telemetry_tasks') }} as t
    where
        t.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(t.task_start_date) >= date('2025-01-01')
    group by 1, 2
),

-- CA cash (comptages espèces, machines à monnayeur) agrégé au mois du comptage
cash_monthly as (
    select
        c.device_id,
        date_trunc(date(c.task_start_date), month) as month_start_date,
        sum(c.ca_cash_ht_eur) as ca_cash_ht_eur,
        sum(c.ca_cash_eur) as ca_cash_ttc_eur
    from {{ ref('int_oracle_lcdp__comptage_tasks') }} as c
    where
        c.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(c.task_start_date) >= date('2025-01-01')
    group by 1, 2
),

joined as (
    select
        coalesce(n.device_id, c.device_id) as device_id,
        coalesce(n.month_start_date, c.month_start_date) as month_start_date,
        coalesce(n.nb_ventes_nayax, 0) as nb_ventes_nayax,
        coalesce(n.ca_nayax_ht_eur, 0) as ca_nayax_ht_eur,
        coalesce(n.ca_nayax_ttc_eur, 0) as ca_nayax_ttc_eur,
        coalesce(c.ca_cash_ht_eur, 0) as ca_cash_ht_eur,
        coalesce(c.ca_cash_ttc_eur, 0) as ca_cash_ttc_eur
    from nayax_monthly as n
    full outer join cash_monthly as c
        on
            n.device_id = c.device_id
            and n.month_start_date = c.month_start_date
)

select
    j.month_start_date,
    j.device_id,
    d.company_id,
    d.has_monnayeur,

    j.nb_ventes_nayax,
    j.ca_nayax_ht_eur,
    j.ca_nayax_ttc_eur,
    j.ca_cash_ht_eur,
    j.ca_cash_ttc_eur,
    j.ca_nayax_ht_eur + j.ca_cash_ht_eur as ca_total_ht_eur,
    j.ca_nayax_ttc_eur + j.ca_cash_ttc_eur as ca_total_ttc_eur

from joined as j
left join devices_perimeter as d on j.device_id = d.device_id
