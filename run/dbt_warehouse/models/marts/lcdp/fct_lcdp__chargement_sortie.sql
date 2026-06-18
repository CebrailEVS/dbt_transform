
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`
      
    partition by week_start_date
    cluster by device_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Taux d'\u00e9coulement (VOLUME) hebdomadaire des machines LCDP t\u00e9l\u00e9m\u00e9tr\u00e9es Nayax FULL NAYAX (sans monnayeur) \u2014 comparaison entr\u00e9es (quantit\u00e9s vendables charg\u00e9es) vs sorties (nombre de ventes Nayax). Mod\u00e8le volume pur, sans mesure mon\u00e9taire (le CA est port\u00e9 par fct_lcdp__ca_mensuel).\n[COMMENT CONSTRUITE] P\u00e9rim\u00e8tre filtr\u00e9 sur dim_lcdp__device.audit_type='1- AUDIT TELEMETRIE (NAYAX)' AND device_category='DA FROID' AND currency_mode='SANS MONNAIE' (full Nayax). Les machines \u00e0 monnayeur (currency_mode='AVEC MONNAIE') sont EXCLUES : une partie de leurs ventes est encaiss\u00e9e en esp\u00e8ces (invisible c\u00f4t\u00e9 Nayax) \u2192 le taux d'\u00e9coulement volume y serait fauss\u00e9 ; leur suivi CA passe par fct_lcdp__ca_mensuel. Entr\u00e9es issues de int_oracle_lcdp__chargement_tasks, class\u00e9es par movement_type (sens d\u00e9riv\u00e9 du SIGNE de la quantit\u00e9, pas du label) : les LOADING alimentent qty_vendable_chargee, les REMOVING (retraits de stock, ex. p\u00e9remption) alimentent qty_vendable_retiree ; jointes \u00e0 dim_lcdp__product et au seed ref_oracle_lcdp__product_group_vendable (filtre is_vendable=true \u2192 3 groupes SNACK / BOISSON FROIDE / PRODUIT FRAIS). Sorties issues de int_oracle_lcdp__telemetry_tasks (1 event = 1 vente). Agr\u00e9gation par device \u00d7 semaine ISO (lundi), FULL OUTER JOIN entre les deux flux. Rolling 4 semaines (W-3 \u00e0 W incluse) calcul\u00e9 via window function born\u00e9e \u00e0 21 jours.\n[GRAIN] 1 ligne par device_id \u00d7 week_start_date.\n[NOTES] Hors p\u00e9rim\u00e8tre : DA CHAUD, DA SEMI-AUTO, BORNE/BROYEUR (chargement non commensurable aux events) et machines \u00e0 monnayeur (volume fauss\u00e9 \u2014 voir fct_lcdp__ca_mensuel pour leur CA). Backfill depuis 2025-01-01. taux_ecoulement_volume = NULL si chargement = 0 (vidange de stock historique attendue). MOD\u00c8LE VOLUME PUR : aucune mesure mon\u00e9taire (CA, co\u00fbt, marge) n'est expos\u00e9e ici (d\u00e9cision 2026-06-17) \u2014 le suivi repose uniquement sur le rapport quantit\u00e9s charg\u00e9es / nombre de ventes. Le CA full-Nayax est disponible dans fct_lcdp__ca_mensuel (grain mensuel). RETRAITS : les REMOVING (produit sorti des machines, ex. p\u00e9remption) sont expos\u00e9s via qty_vendable_retiree (volume d'articles, pas de count d'op\u00e9rations) ; le taux d'\u00e9coulement NET (taux_ecoulement_volume_net_4wk = ventes / (charg\u00e9 \u2212 retir\u00e9)) corrige le d\u00e9nominateur. Classement charg\u00e9/retir\u00e9 pilot\u00e9 par movement_type (signe de la quantit\u00e9, cf. int_oracle_lcdp__chargement_tasks).\n"""
    )
    as (
      

-- Taux d'écoulement (VOLUME) des DA FROID full Nayax, par device × semaine ISO.
-- Modèle VOLUME PUR : entrées (qty vendable chargée / retirée) vs sorties (nb ventes Nayax).
-- Aucune mesure monétaire ici : le CA est porté par fct_lcdp__ca_mensuel (tout le
-- parc + cash). Périmètre limité au full Nayax car sur les machines à monnayeur
-- une partie des ventes part en espèces (invisible côté Nayax) → le volume serait faux.
-- Mouvements de stock classés par movement_type (signe de la quantité, cf.
-- int_oracle_lcdp__chargement_tasks) : LOADING = chargé, REMOVING = retiré
-- (produit sorti de la machine, ex. péremption). Le taux d'écoulement NET tient
-- compte du retiré au dénominateur : ventes / (chargé − retiré).

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
        sum(case when c.movement_type = 'LOADING' then c.load_quantity else 0 end)
            as qty_vendable_chargee,
        sum(case when c.movement_type = 'REMOVING' then -c.load_quantity else 0 end)
            as qty_vendable_retiree
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__chargement_tasks` as c
    inner join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__product` as p
        on c.product_id = p.product_id
    inner join vendable_groups as v
        on trim(p.product_group) = v.product_group
    where
        c.device_id in (select dp.device_id from devices_perimeter as dp)
        and date(c.task_start_date) >= date('2025-01-01')
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
        coalesce(c.qty_vendable_retiree, 0) as qty_vendable_retiree,
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
        qty_vendable_retiree,
        nb_ventes,

        sum(qty_vendable_chargee) over wk4 as qty_vendable_chargee_4wk,
        sum(qty_vendable_retiree) over wk4 as qty_vendable_retiree_4wk,
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
    qty_vendable_retiree,

    -- Briques additives (sorties)
    nb_ventes,

    -- Ratio volume (non additif — fourni pour confort, recalcul possible en BI)
    safe_divide(nb_ventes, qty_vendable_chargee) as taux_ecoulement_volume_hebdo,

    -- Briques additives rolling 4 semaines
    qty_vendable_chargee_4wk,
    qty_vendable_retiree_4wk,
    nb_ventes_4wk,
    safe_divide(nb_ventes_4wk, qty_vendable_chargee_4wk) as taux_ecoulement_volume_4wk,

    -- Taux d'écoulement NET 4 sem. : tient compte du retiré au dénominateur.
    -- NULL si retiré >= chargé (dénominateur <= 0, fenêtre de déstockage non interprétable).
    safe_divide(nb_ventes_4wk, nullif(greatest(qty_vendable_chargee_4wk - qty_vendable_retiree_4wk, 0), 0))
        as taux_ecoulement_volume_net_4wk

from with_rolling
    );
  