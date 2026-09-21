
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__amortissement_machines`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dotation aux amortissements mensuelle, par machine et par client : la machine elle-m\u00eame et ses sous-\u00e9quipements.\n[COMMENT CONSTRUITE] Machines (device de type 1) vues sur une t\u00e2che du mois aux statuts 1/4/5/7, chez un client sous contrat avant le mois. Dotation lin\u00e9aire = purchase_cost / damping_duration, arr\u00eat\u00e9e quand l'amortissement est termin\u00e9 avant le mois. Dur\u00e9es par d\u00e9faut : 36 mois pour la machine, 60 pour un sous-\u00e9quipement. Les sous-\u00e9quipements sont agr\u00e9g\u00e9s AVANT jointure.\n[GRAIN] 1 ligne par (mois, company_id, device_id).\n[NOTES] \u00c9CART VOLONTAIRE avec Distrilog \u2014 d\u00e9tail dans le YAML de fct_neshu__pnl_client_mensuel, note (2). En r\u00e9sum\u00e9 : leur requ\u00eate groupe les sous-\u00e9quipements par la VALEUR de la dotation et non par leur identifiant, ce qui fusionne ceux qui co\u00fbtent pareil et d\u00e9multiplie la dotation de la machine quand ils diff\u00e8rent. Ici chaque sous-\u00e9quipement compte une fois et chaque machine une fois. La date de r\u00e9f\u00e9rence est le premier jour du mois de la ligne et non la date du calcul, ce qui rend le mod\u00e8le rejouable. Une machine sans purchase_cost a une dotation nulle : donn\u00e9e absente \u00e0 la source.\n"""
    )
    as (
      

-- Dotation linéaire d'une machine (device de type 1) vue chez un client dans le
-- mois, augmentée de celle de ses sous-équipements rattachés.
--
-- Deux durées par défaut, reprises de l'ERP : 36 mois pour la machine, 60 pour
-- un sous-équipement. Une durée à zéro sur un sous-équipement est traitée comme
-- 60 — c'est le comportement du rapport Distrilog.
--
-- ⚠️ La dotation s'arrête quand l'amortissement est terminé AVANT le mois
-- observé. La date de référence est donc le premier jour du mois de la ligne, et
-- non la date du calcul : un mois ancien recalculé aujourd'hui redonne le même
-- montant. Le rapport Distrilog, lui, utilise la borne basse de la période
-- demandée — même résultat mois par mois, mais rejouable ici.
--
-- Écart assumé avec le rapport : Distrilog joint les sous-équipements par une
-- jointure externe qui DÉMULTIPLIE la dotation de la machine quand elle en porte
-- plusieurs. Ici les sous-équipements sont agrégés d'abord, la machine n'est
-- comptée qu'une fois.

with machines_du_mois as (

    -- Une machine est amortie chez le client où elle a été vue dans le mois.
    select distinct
        date_trunc(date(t.real_start_date), month) as mois,
        d.iddevice as device_id,
        d.code as device_code,
        c.idcompany as company_id,
        c.code as company_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
        on t.iddevice = d.iddevice and d.iddevice_type = 1  -- MACHINE
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on t.idcompany_peer = c.idcompany and c.idcompany_type = 2  -- CUSTOMER
    inner join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__contrat_client` as cc
        on
            c.idcompany = cc.company_id
            and t.real_start_date > timestamp(date_trunc(date(cc.first_contract_date), month))
    where
        t.idtask_status in (1, 4, 5, 7)  -- FAIT, VALIDE, ANOMALIE, ACQUITTE
        and t.real_start_date is not null
),

dotation_machine as (

    select
        m.mois,
        m.device_id,
        m.device_code,
        m.company_id,
        m.company_code,
        case
            when date_add(
                coalesce(date(d.start_date_credit), date(d.purchase_date)),
                interval coalesce(d.damping_duration, 36) month
            ) < m.mois then 0
            else round(d.purchase_cost / coalesce(nullif(d.damping_duration, 0), 36), 2)
        end as dotation_machine_eur
    from machines_du_mois as m
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d on m.device_id = d.iddevice
),

dotation_sous_equipements as (

    -- Agrégés AVANT la jointure : une machine qui porte trois sous-équipements
    -- ne doit pas voir sa propre dotation comptée trois fois.
    select
        m.mois,
        m.device_id,
        sum(
            round(dd.purchase_cost / coalesce(nullif(dd.damping_duration, 0), 60), 2)
        ) as dotation_accessoires_eur
    from machines_du_mois as m
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as dd
        on m.device_id = dd.device_iddevice
        -- Seuls les sous-équipements dont l'amortissement court encore.
        and date_add(
            coalesce(date(dd.start_date_credit), date(dd.purchase_date)),
            interval coalesce(dd.damping_duration, 60) month
        ) >= m.mois
    group by m.mois, m.device_id
)

select
    dm.mois,
    dm.company_id,
    dm.company_code,
    dm.device_id,
    dm.device_code,
    coalesce(dm.dotation_machine_eur, 0) as dotation_machine_eur,
    coalesce(dse.dotation_accessoires_eur, 0) as dotation_accessoires_eur,
    coalesce(dm.dotation_machine_eur, 0)
    + coalesce(dse.dotation_accessoires_eur, 0) as montant_amortissement_eur
from dotation_machine as dm
left join dotation_sous_equipements as dse
    on dm.mois = dse.mois and dm.device_id = dse.device_id
    );
  