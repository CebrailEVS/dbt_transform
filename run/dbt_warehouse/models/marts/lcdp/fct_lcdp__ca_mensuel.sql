
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_lcdp__ca_mensuel`
      
    partition by month_start_date
    cluster by device_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Chiffre d'affaires mensuel des machines LCDP t\u00e9l\u00e9m\u00e9tr\u00e9es Nayax (DA FROID), consolidant le CA Nayax et le CA encaiss\u00e9 en esp\u00e8ces (machines \u00e0 monnayeur). Vue revenue pure (sans marge).\n[COMMENT CONSTRUITE] P\u00e9rim\u00e8tre dim_lcdp__device.audit_type='1- AUDIT TELEMETRIE (NAYAX)' AND device_category='DA FROID' (machines AVEC et SANS monnayeur). CA Nayax = \u03a3 sale_amount_net_eur (HT) / sale_amount_net_tax_eur (TTC) de int_oracle_lcdp__telemetry_tasks, agr\u00e9g\u00e9 au mois. CA cash = \u03a3 ca_cash_ht_eur / ca_cash_eur de int_oracle_lcdp__comptage_tasks (pi\u00e8ces + billets, machines \u00e0 monnayeur), au mois du comptage. FULL OUTER JOIN device \u00d7 mois entre les deux flux ; CA total = Nayax + cash. has_monnayeur depuis currency_mode. Backfill depuis 2025-01-01.\n[GRAIN] 1 ligne par device_id \u00d7 month_start_date (1er jour du mois).\n[NOTES] ATTRIBUTION TEMPORELLE \u2014 deux flux de CA \u00e0 temporalit\u00e9s diff\u00e9rentes, r\u00e9concili\u00e9s par le grain mensuel. (1) CA Nayax : rattach\u00e9 au MOIS EXACT de la vente (chaque event t\u00e9l\u00e9m\u00e9trie est horodat\u00e9). (2) CA cash : rattach\u00e9 au mois de la t\u00e2che REGL COMPTAGE, PAS au mois r\u00e9el des ventes esp\u00e8ces. Un comptage rel\u00e8ve le cash accumul\u00e9 dans le monnayeur depuis le comptage pr\u00e9c\u00e9dent, \u00e0 cadence IRR\u00c9GULI\u00c8RE (~1-2 sem) ; le CA d'un comptage couvre donc une fen\u00eatre \u00e0 cheval sur plusieurs jours/semaines, parfois deux mois. POURQUOI LE GRAIN MENSUEL : il absorbe cette irr\u00e9gularit\u00e9 \u2014 \u00e0 l'\u00e9chelle du mois, le d\u00e9calage de quelques jours d'un comptage en fin/d\u00e9but de mois reste marginal (fuite de bord faible), alors qu'un grain hebdo serait fauss\u00e9 par la cadence al\u00e9atoire des comptages. NE PAS descendre sous le mensuel. ca_total = CA Nayax (mois de vente) + CA cash (mois de comptage) : addition assum\u00e9e malgr\u00e9 les temporalit\u00e9s l\u00e9g\u00e8rement diff\u00e9rentes. Pour full Nayax (has_monnayeur=false), ca_cash_*=0 et le mois est exact. REVENUE PUR : aucun co\u00fbt / marge / COGS n'est calcul\u00e9 ici (d\u00e9cision m\u00e9tier 2026-06-17). Le co\u00fbt d'achat des chargements n'est volontairement PAS joint : son timing (mois de chargement) diff\u00e8re de celui des ventes et brouillerait la lecture CA ; le suivi marge/\u00e9coulement vit dans fct_lcdp__chargement_sortie (full Nayax). Le COGS du cash serait de toute fa\u00e7on inconnu (produit vendu en esp\u00e8ces non d\u00e9taill\u00e9). Pas de volume cash (pi\u00e8ces non compt\u00e9es). Montants HT et TTC expos\u00e9s. Backfill depuis 2025-01-01.\n"""
    )
    as (
      

-- CA mensuel des DA FROID Nayax (parc complet : avec ET sans monnayeur).
-- Deux flux de CA à temporalités différentes, réconciliés par le grain MENSUEL :
--   1. CA Nayax  → rattaché au mois EXACT de la vente (event télémétrie horodaté).
--   2. CA cash   → rattaché au mois de la tâche REGL COMPTAGE, pas au mois réel
--                  des ventes. Le comptage relève le cash accumulé depuis le
--                  comptage précédent, à cadence irrégulière (~1-2 sem).
-- Le mensuel est choisi pour absorber l'irrégularité des comptages (fuite de bord
-- faible vs un grain hebdo qui serait faussé). Ne pas descendre sous le mensuel.
-- Vue REVENUE PUR : aucun coût/marge ici (le suivi marge/écoulement vit dans
-- fct_lcdp__chargement_sortie, full Nayax). ca_total = CA Nayax + CA cash.

with devices_perimeter as (
    select
        device_id,
        company_id,
        coalesce(currency_mode = 'AVEC MONNAIE', false) as has_monnayeur
    from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`
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
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__telemetry_tasks` as t
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
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__comptage_tasks` as c
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
    );
  