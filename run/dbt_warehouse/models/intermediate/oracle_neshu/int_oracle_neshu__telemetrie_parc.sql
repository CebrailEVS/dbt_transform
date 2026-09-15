
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetrie_parc`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Co\u00fbt mensuel de t\u00e9l\u00e9m\u00e9trie par client, d\u00e9duit du parc de terminaux de paiement observ\u00e9 chez lui.\n[COMMENT CONSTRUITE] T\u00e2ches DESTRUCTION (271) et INSTALL MACHINE (134) portant un device de type 4, jointes au produit du mod\u00e8le (device.idmodel = product.idproduct, types 4 et 7 actifs). Co\u00fbt unitaire lu dans la zone XML du produit au noeud /ZONE/CLOC, multipli\u00e9 par le nombre de terminaux distincts. Client sous contrat avant le mois observ\u00e9.\n[GRAIN] 1 ligne par (mois, company_id).\n[NOTES] Le parc est observ\u00e9 \u00e0 travers des \u00c9V\u00c9NEMENTS (destruction, installation) et non un inventaire : un client dont le parc n'a pas boug\u00e9 dans le mois n'a aucun co\u00fbt de t\u00e9l\u00e9m\u00e9trie \u2014 25 lignes sur 180 \u00e0 z\u00e9ro en ao\u00fbt 2026. Comportement de Distrilog reproduit tel quel ; savoir s'il est voulu est une question ouverte c\u00f4t\u00e9 m\u00e9tier. La vue Oracle `model` n'est pas r\u00e9pliqu\u00e9e : c'est `product` filtr\u00e9 sur les types 4 et 7 actifs, et la correspondance device.idmodel = product.idproduct tient (137 mod\u00e8les, 137 produits appari\u00e9s). Valid\u00e9 exact contre le rapport Distrilog sur ao\u00fbt 2026.\n"""
    )
    as (
      

-- Le coût de télémétrie n'est pas facturé ligne à ligne : il est déduit du PARC
-- de terminaux de paiement (device de type 4) observé chez le client, multiplié
-- par le coût mensuel du modèle, rangé dans la zone XML du produit au noeud
-- /ZONE/CLOC.
--
-- ⚠️ Le parc est observé à travers les tâches de type DESTRUCTION (271) et
-- INSTALL MACHINE (134) — deux ÉVÉNEMENTS, pas un inventaire. Un client dont le
-- parc n'a pas bougé dans le mois n'a donc aucun coût de télémétrie : 25 lignes
-- sur 180 sont à zéro en août 2026. C'est le comportement du rapport Distrilog,
-- reproduit tel quel ; la question de savoir s'il est voulu est ouverte côté métier.
--
-- La vue Oracle `model` n'est pas répliquée : c'est `product` filtré sur les
-- types 4 et 7, actifs. La jointure device.idmodel = product.idproduct tient
-- (137 modèles, 137 produits appariés au 2026-09-15).

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
    );
  