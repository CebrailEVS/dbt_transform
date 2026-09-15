

-- Le coût de télémétrie n'est pas facturé ligne à ligne : il est déduit du PARC
-- de terminaux de paiement (device de type 4) observé chez le client, multiplié
-- par le coût mensuel du modèle, rangé dans la zone XML du produit au noeud
-- /ZONE/CLOC.
--
-- ⚠️ Le parc est observé à travers les tâches de type DESTRUCTION (271) et
-- INSTALL MACHINE (134) — deux ÉVÉNEMENTS, pas un inventaire. Un client dont le
-- parc n'a pas bougé dans le mois n'a donc aucun coût de télémétrie. C'est le
-- comportement du rapport Distrilog, reproduit tel quel ; savoir s'il est voulu
-- est une question ouverte côté métier.
--
-- La vue Oracle `model` n'est pas répliquée : c'est `product` filtré sur les
-- types 4 et 7, actifs, et device.idmodel pointe directement product.idproduct.

with parc_par_modele as (

    select
        date_trunc(date(t.real_start_date), month) as mois,
        c.idcompany as company_id,
        c.code as company_code,
        p.code as model_code,
        count(distinct d.code) as nb_terminaux,
        safe_cast(
            regexp_extract(p.xml, r'<CLOC>([^<]*)</CLOC>') as float64
        ) as cout_mensuel_unitaire_eur

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
        on t.iddevice = d.iddevice and d.iddevice_type = 4  -- SYSTEME DE PAIEMENT
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as p
        on
            d.idmodel = p.idproduct
            and p.idproduct_type in (4, 7)
            -- numérique ici, contrairement à `task` où le staging le caste en texte
            and p.code_status_record = 1
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on t.idcompany_peer = c.idcompany and c.idcompany_type = 2  -- CUSTOMER
    -- Le client doit être sous contrat avant le mois observé.
    inner join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__contrat_client` as cc
        on
            c.idcompany = cc.company_id
            and t.real_start_date > timestamp(date_trunc(date(cc.first_contract_date), month))

    where
        t.idtask_type in (271, 134)  -- DESTRUCTION, INSTALL MACHINE
        and t.real_start_date is not null

    group by mois, company_id, company_code, model_code, cout_mensuel_unitaire_eur
)

select
    mois,
    company_id,
    company_code,
    sum(nb_terminaux) as nb_terminaux,
    sum(nb_terminaux * coalesce(cout_mensuel_unitaire_eur, 0)) as cout_telemetrie_eur
from parc_par_modele
group by mois, company_id, company_code