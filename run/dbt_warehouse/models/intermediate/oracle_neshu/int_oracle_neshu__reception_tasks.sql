
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__reception_tasks`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] R\u00e9ceptions de marchandises NESHU : une ligne par produit sur un bon de r\u00e9ception (entr\u00e9e de stock dans un d\u00e9p\u00f4t). Suivi des quantit\u00e9s et valeurs re\u00e7ues.\n[COMMENT CONSTRUITE] stg_oracle_neshu__task (idtask_type = 121 BON RECEPTION, statuts FAIT/VALIDE/ANNULE/ANOMALIE, code_status_record = '1') crois\u00e9 avec task_has_product (grain ligne produit), enrichi product. La destination est r\u00e9solue quand product_destination_type = COMPANY \u2192 code d\u00e9p\u00f4t (company). Quantit\u00e9 ramen\u00e9e en unit\u00e9s de base.\n[GRAIN] 1 ligne par task_product_id (ligne produit d'un bon de r\u00e9ception). ~9 563 lignes (~1 693 bons), depuis 2022.\n[NOTES] Sym\u00e9trique de la livraison mais c\u00f4t\u00e9 entr\u00e9e : product_destination_id est polymorphe et vaut toujours COMPANY ici (r\u00e9ception dans un d\u00e9p\u00f4t). Pas de source. Enrichi avec la date de la commande fournisseur (type 120) rattach\u00e9e pour calculer le d\u00e9lai de livraison : le lien passe par le document parent partag\u00e9 (task_idtask), une r\u00e9ception pointe vers au plus 1 commande. Certaines r\u00e9ceptions n'ont pas de commande rapprochable (commande_start_date et d\u00e9lai NULL), et de rares saisies incoh\u00e9rentes donnent un d\u00e9lai n\u00e9gatif (r\u00e9ception dat\u00e9e avant la commande) : filtrer sur is_lead_time_valid. Le statut de livraison (LIVRE/PARTIEL/EN_ATTENTE), saisi manuellement c\u00f4t\u00e9 ERP, est peu fiable et n'est pas repris ici \u2014 tout \u00e9cart de compl\u00e9tude quantit\u00e9 se traite c\u00f4t\u00e9 m\u00e9tier.\n"""
    )
    as (
      

with commande_header as (

    -- Date de la commande fournisseur (type 120) rattachée à la réception.
    -- Le lien passe par le document parent partagé (task_idtask) : commande et réception
    -- pointent vers le même parent. 1 commande par parent -> au plus 1 date par réception,
    -- donc pas de fan-out. min() = collapse défensif des lignes produit de la commande.
    select
        task_idtask as parent_id,
        min(real_start_date) as commande_start_date

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task`
    where
        idtask_type = 120  -- COMMANDE FOURNISSEUR
        and code_status_record = '1'
        and task_idtask is not null
    group by task_idtask
),

reception_base as (

    select
        -- Identifiants
        thp.idtask_has_product as task_product_id,
        t.idtask as task_id,
        t.task_idtask as parent_id,  -- interne : clé de jointure vers la commande (non exposé)
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idproduct_destination as product_destination_id,
        t.type_product_destination as product_destination_type,

        -- Codes
        cd.code as destination_code,
        p.code as product_code,
        ts.code as task_status_code,

        -- Informations date
        t.real_start_date as task_start_date,

        -- Informations de conditionnement
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity as base_unit_quantity,
        thp.net_price as product_unit_price_task,
        p.purchase_unit_price as product_unit_price_latest,

        -- Métriques
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) as quantity,
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div * p.purchase_unit_price) as valuation,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp
        on t.idtask = thp.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as p
        on thp.idproduct = p.idproduct
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` as ts
        on t.idtask_status = ts.idtask_status

    -- Destination = COMPANY uniquement
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as cd
        on
            t.idproduct_destination = cd.idcompany
            and t.type_product_destination = 'COMPANY'

    where
        1 = 1
        and t.idtask_status in (1, 4, 3, 5)  -- FAIT, VALIDE, ANNULE, ANOMALIE
        and t.code_status_record = '1'
        and t.idtask_type = 121 -- BON RECEPTION
        and t.real_start_date is not null

    group by
        thp.idtask_has_product, t.idtask, t.task_idtask, t.idcompany_peer,
        t.idproduct_destination, t.type_product_destination,
        thp.idproduct,
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity,
        thp.net_price,
        p.purchase_unit_price,
        cd.code,
        p.code, ts.code,
        t.real_start_date,
        t.updated_at, t.created_at, t.extracted_at
)

select
    -- Identifiants
    rb.task_product_id,
    rb.task_id,
    rb.company_id,
    rb.product_id,
    rb.product_destination_id,
    rb.product_destination_type,

    -- Codes
    rb.destination_code,
    rb.product_code,
    rb.task_status_code,

    -- Infos métier
    rb.task_start_date,
    ch.commande_start_date,

    -- Délai de livraison fournisseur (commande -> réception)
    date_diff(date(rb.task_start_date), date(ch.commande_start_date), day) as delivery_lead_time_days,
    ch.commande_start_date is not null
    and rb.task_start_date >= ch.commande_start_date as is_lead_time_valid,

    -- Infos de conditionnement
    rb.unit_coeff_multi,
    rb.unit_coeff_div,
    rb.base_unit_quantity,
    rb.product_unit_price_task,
    rb.product_unit_price_latest,

    -- Métriques
    rb.quantity,
    rb.valuation,

    -- Timestamps techniques
    rb.updated_at,
    rb.created_at,
    rb.extracted_at

from reception_base as rb
left join commande_header as ch
    on rb.parent_id = ch.parent_id
    );
  