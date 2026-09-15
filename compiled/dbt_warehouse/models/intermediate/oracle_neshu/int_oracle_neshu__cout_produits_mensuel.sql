

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