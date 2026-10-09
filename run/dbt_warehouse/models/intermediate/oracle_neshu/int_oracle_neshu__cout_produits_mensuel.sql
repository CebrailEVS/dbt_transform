
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__cout_produits_mensuel`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Co\u00fbt des produits mis en machine et livr\u00e9s en bon de livraison, par client et par mois.\n[COMMENT CONSTRUITE] Deux calculs r\u00e9unis par full outer join. Charg\u00e9 : somme de amount_without_tax de stg_oracle_neshu__task_has_amount sur les t\u00e2ches CHARGEMENT MACHINE (type 13). Livr\u00e9 : real_quantity \u00d7 unit_coeff_multi / unit_coeff_div \u00d7 purchase_unit_price sur les t\u00e2ches BL CLIENT (type 101). Statuts 1/4 dans les deux cas.\n[GRAIN] 1 ligne par (mois, company_id).\n[NOTES] POURQUOI ce mod\u00e8le plut\u00f4t que int_oracle_neshu__chargement_tasks et __livraison_tasks, qui portent les m\u00eames types de t\u00e2che. D'abord leurs filtres de statut incluent 3 ANNULE et 5 ANOMALIE : ils servent le suivi d'activit\u00e9, qui veut voir ce qui a \u00e9t\u00e9 tent\u00e9, alors que le P&L ne compte que le r\u00e9alis\u00e9. Ensuite, pour le chargement, la valorisation diff\u00e8re \u00e0 la racine : ces mod\u00e8les estiment au prix d'achat courant du produit, le P&L lit le montant HT r\u00e9ellement constat\u00e9 sur la t\u00e2che. Les deux colonnes sont valid\u00e9es contre le rapport Distrilog.\n"""
    )
    as (
      

-- Les deux coûts de produits du P&L client, au grain (mois, client).
--
-- ⚠️ Pourquoi ne PAS réutiliser int_oracle_neshu__chargement_tasks et
-- int_oracle_neshu__livraison_tasks, qui portent les mêmes types de tâche :
--
--   1. Leurs filtres de statut incluent 3 ANNULE et 5 ANOMALIE. Ils servent le
--      suivi d'activité, qui veut voir ce qui a été tenté. Le P&L ne compte que
--      le réalisé : statuts 1 FAIT et 4 VALIDE.
--   2. Pour le chargement, la valorisation diffère à la racine. Ces modèles
--      valorisent par le prix d'achat courant du produit
--      (real_quantity * coeff * purchase_unit_price) ; le P&L lit le montant HT
--      réellement porté par la tâche, dans task_has_amount. Le premier est une
--      estimation au tarif du jour, le second un montant constaté.
--
-- La livraison, elle, suit bien la formule de conversion d'unité du dépôt —
-- seul le périmètre de statuts change.

with cout_charge as (

    -- Coût des produits mis en machine : montant HT constaté sur la tâche.
    select
        date_trunc(date(t.real_start_date), month) as mois,
        c.idcompany as company_id,
        c.code as company_code,
        round(sum(tha.amount_without_tax), 2) as cout_produits_charges_ht_eur
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_amount` as tha
        on t.idtask = tha.idtask
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on t.idcompany_peer = c.idcompany
    where
        t.idtask_type = 13  -- CHARGEMENT MACHINE
        and t.idtask_status in (1, 4)  -- FAIT, VALIDE
        and t.code_status_record = '1'
        and t.real_start_date is not null
    group by mois, company_id, company_code
),

cout_livre as (

    -- Coût des produits livrés en bon de livraison, au prix d'achat, converti
    -- dans l'unité de la ligne.
    select
        date_trunc(date(t.real_start_date), month) as mois,
        c.idcompany as company_id,
        c.code as company_code,
        round(
            sum(
                thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div
                * thp.purchase_unit_price
            ), 2
        ) as cout_produits_livres_ht_eur
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp
        on t.idtask = thp.idtask
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on t.idcompany_peer = c.idcompany
    where
        t.idtask_type = 101  -- BL CLIENT
        and t.idtask_status in (1, 4)  -- FAIT, VALIDE
        and t.code_status_record = '1'
        and t.real_start_date is not null
    group by mois, company_id, company_code
)

select
    coalesce(ch.mois, li.mois) as mois,
    coalesce(ch.company_id, li.company_id) as company_id,
    coalesce(ch.company_code, li.company_code) as company_code,
    coalesce(ch.cout_produits_charges_ht_eur, 0) as cout_produits_charges_ht_eur,
    coalesce(li.cout_produits_livres_ht_eur, 0) as cout_produits_livres_ht_eur
from cout_charge as ch
full outer join cout_livre as li
    on ch.mois = li.mois and ch.company_id = li.company_id
    );
  