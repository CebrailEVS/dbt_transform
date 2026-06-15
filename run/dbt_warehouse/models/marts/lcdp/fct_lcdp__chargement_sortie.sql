
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`
      
    partition by week_start_date
    cluster by device_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Taux d'\u00e9coulement hebdomadaire des machines LCDP t\u00e9l\u00e9m\u00e9tr\u00e9es Nayax \u2014 comparaison entr\u00e9es (chargements vendables) vs sorties (events Nayax PLANIFIED_SAILS) en volume et en \u20ac.\n[COMMENT CONSTRUITE] P\u00e9rim\u00e8tre filtr\u00e9 sur dim_lcdp__device.audit_type='1- AUDIT TELEMETRIE (NAYAX)' AND device_category='DA FROID'. Entr\u00e9es issues de int_oracle_lcdp__chargement_tasks filtr\u00e9es sur load_type_code='LOADING' (les REMOVING = retraits de stock sont exclus pour ne pas fausser le flux d'entr\u00e9e), jointes \u00e0 dim_lcdp__product et au seed ref_oracle_lcdp__product_group_vendable (filtre is_vendable=true \u2192 3 groupes SNACK / BOISSON FROIDE / PRODUIT FRAIS). Sorties issues de int_oracle_lcdp__telemetry_tasks (1 event = 1 vente, sale_amount_net remont\u00e9 par Nayax). Agr\u00e9gation par device \u00d7 semaine ISO (lundi), FULL OUTER JOIN entre les deux flux. Rolling 4 semaines (W-3 \u00e0 W incluse) calcul\u00e9 via window function born\u00e9e \u00e0 21 jours.\n[GRAIN] 1 ligne par device_id \u00d7 week_start_date.\n[NOTES] Hors p\u00e9rim\u00e8tre V1 : DA CHAUD, DA SEMI-AUTO et BORNE/BROYEUR (chargement non commensurable aux events). Backfill depuis 2025-01-01. taux_ecoulement_volume = NULL si chargement = 0 (vidange de stock historique attendue). MARGE : le mart ne fige PAS de taux de marge brute (un ratio par semaine n'est pas additif et le co\u00fbt de r\u00e9f\u00e9rence d\u00e9pend de la p\u00e9riode analys\u00e9e). La marge se calcule en BI, align\u00e9e sur la p\u00e9riode filtr\u00e9e : co\u00fbt_unitaire = \u03a3 cout_achat_chargement_eur / \u03a3 qty_vendable_chargee sur la p\u00e9riode \u2192 COGS = \u03a3 nb_ventes \u00d7 co\u00fbt_unitaire \u2192 taux_marge_brute = (\u03a3 CA HT - COGS) / \u03a3 CA HT. Repli sur cout_unitaire_achat_repli_eur (moyenne historique par device) quand la p\u00e9riode n'a aucun chargement. Le COGS est estim\u00e9 car le produit vendu est IND\u00c9FINI c\u00f4t\u00e9 Nayax (on suppose mix vendu = mix charg\u00e9). Taux de marge brute typique \u2248 50-55%. Les chargements REMOVING (retraits de stock) sont exclus \u2014 tra\u00e7abilit\u00e9 via int_oracle_lcdp__chargement_tasks.\n"""
    )
    as (
      

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
),

with_unit_cost as (
    select
        wr.*,
        -- Coût d'achat unitaire de REPLI par device (moyenne sur tout l'historique).
        -- Utilisé en BI uniquement comme fallback pour estimer le COGS quand la période
        -- analysée n'a aucun chargement. Le calcul nominal de la marge se fait en BI,
        -- aligné sur la période (coût unitaire = Σ coût / Σ qty sur la période filtrée),
        -- car le mart est période-agnostique et un taux figé ne serait pas additif.
        safe_divide(
            sum(wr.cout_achat_chargement_eur) over (partition by wr.device_id),
            sum(wr.qty_vendable_chargee) over (partition by wr.device_id)
        ) as cout_unitaire_achat_repli_eur
    from with_rolling as wr
)

select
    device_id,
    week_start_date,

    -- Briques additives (entrées)
    qty_vendable_chargee,
    cout_achat_chargement_eur,
    cout_unitaire_achat_repli_eur,
    nb_chargements,

    -- Briques additives (sorties)
    nb_ventes,
    ca_ht_sortie_eur,
    ca_ttc_sortie_eur,

    -- Ratio volume (non additif — fourni pour confort, recalcul possible en BI)
    safe_divide(nb_ventes, qty_vendable_chargee) as taux_ecoulement_volume_hebdo,

    -- Briques additives rolling 4 semaines
    qty_vendable_chargee_4wk,
    cout_achat_chargement_4wk_eur,
    nb_ventes_4wk,
    ca_ht_sortie_4wk_eur,
    ca_ttc_sortie_4wk_eur,
    safe_divide(nb_ventes_4wk, qty_vendable_chargee_4wk) as taux_ecoulement_volume_4wk

from with_unit_cost
    );
  