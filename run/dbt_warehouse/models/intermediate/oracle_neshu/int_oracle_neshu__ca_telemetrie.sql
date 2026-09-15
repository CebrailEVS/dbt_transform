
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__ca_telemetrie`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Chiffre d'affaires des ventes relev\u00e9es sur les machines par la t\u00e9l\u00e9m\u00e9trie (Nayax), hors distributions gratuites. Sixi\u00e8me composante du CA du P&L client.\n[COMMENT CONSTRUITE] T\u00e2ches AUDITTELE (type 3), statuts 1/4, jointes \u00e0 task_has_product puis au label de la LIGNE de produit via stg_oracle_neshu__label_has_thp, filtr\u00e9es sur la famille PAYMENT_MODE avec un code diff\u00e9rent de FREE.\n[GRAIN] 1 ligne par (mois, company_id).\n[NOTES] Deux \u00e9carts de r\u00e8gle avec les cinq autres composantes, repris tels quels de Distrilog : les statuts retenus sont 1 et 4 seulement (la facturation accepte aussi 2 ENCOURS), et aucun coefficient de signe ne s'applique. Le filtre sur le mode de paiement est ce qui distingue une vente d'une distribution gratuite ; ce lien n'existe que dans label_has_thp. NE PAS confondre avec int_oracle_neshu__telemetry_tasks, qui porte le m\u00eame type de t\u00e2che mais filtre sur les labels TELEM_SOURCE de la T\u00c2CHE. Valid\u00e9 au centime contre le rapport Distrilog.\n"""
    )
    as (
      

-- Sixième composante du chiffre d'affaires, et la seule qui ne passe pas par une
-- facture : ce sont les ventes relevées sur les machines par la télémétrie
-- (tâches AUDITTELE, type 3).
--
-- Deux écarts de règle avec les cinq autres branches, repris tels quels du
-- rapport Distrilog :
--   - les statuts retenus sont 1 et 4 seulement, là où la facturation accepte
--     aussi 2 ENCOURS ;
--   - aucun coefficient de signe n'est appliqué (il n'y a pas d'avoir ici).
--
-- Le filtre qui compte est le mode de paiement porté par la LIGNE de produit :
-- une distribution gratuite (PAYMENT_MODE = FREE) ne fait pas de chiffre
-- d'affaires. C'est ce lien que la table label_has_thp apporte — il n'existe
-- nulle part ailleurs.
--
-- Ne pas confondre avec int_oracle_neshu__telemetry_tasks, qui porte le même
-- type de tâche mais filtre sur les labels TELEM_SOURCE de la TÂCHE, pas sur le
-- mode de paiement de la ligne.

with ventes_telemetrie as (

    select
        date_trunc(date(t.real_start_date), month) as mois,
        c.idcompany as company_id,
        c.code as company_code,
        thp.sale_amount_net,
        t.updated_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on t.idcompany_peer = c.idcompany
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp
        on t.idtask = thp.idtask
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_thp` as lht
        on thp.idtask_has_product = lht.idtask_has_product
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lht.idlabel = l.idlabel
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family

    where
        t.idtask_type = 3  -- AUDITTELE
        and t.idtask_status in (1, 4)  -- FAIT, VALIDE
        and t.code_status_record = '1'
        and t.real_start_date is not null
        and lf.code = 'PAYMENT_MODE'
        and l.code != 'FREE'
)

select
    mois,
    company_id,
    company_code,
    round(sum(sale_amount_net), 2) as ca_nayax_ht_eur,
    count(*) as nb_ventes,
    max(updated_at) as updated_at,
    max(extracted_at) as extracted_at
from ventes_telemetrie
group by mois, company_id, company_code
    );
  