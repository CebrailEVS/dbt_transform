
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__sortie_fabrication_tasks`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Sorties de fabrication LCDP (type 297) : une ligne par produit de caf\u00e9 torr\u00e9fi\u00e9 / m\u00e9lange produit par la torr\u00e9faction. C'est l'extrant du process \u2014 le caf\u00e9 torr\u00e9fi\u00e9 sort du d\u00e9p\u00f4t FABRICATION (source) vers le d\u00e9p\u00f4t de caf\u00e9 torr\u00e9fi\u00e9 (destination, typiquement TORREFIEVRAC).\n[COMMENT CONSTRUITE] stg_oracle_lcdp__task (idtask_type = 297, code_status_record = '1', real_start_date non nul, tous statuts conserv\u00e9s) crois\u00e9 avec task_has_product (grain ligne produit), enrichi product et task_status. Source et destination r\u00e9solues sur company (COMPANY dans 100 % des cas). Seules les lignes dont source ET destination se r\u00e9solvent sont conserv\u00e9es. Quantit\u00e9 ramen\u00e9e en unit\u00e9s de base (real_quantity \u00d7 coeff_multi / coeff_div).\n[GRAIN] 1 ligne par task_product_id (ligne produit d'une sortie de fabrication). ~2 300 lignes (~347 t\u00e2ches, 47 produits), depuis janvier 2025.\n[NOTES] Maillon aval de la cha\u00eene de torr\u00e9faction : VERT (caf\u00e9 vert) \u2192 [ENTREE_FABRICATION] \u2192 FABRICATION \u2192 [SORTIE_FABRICATION] \u2192 TORREFIEVRAC (caf\u00e9 torr\u00e9fi\u00e9, codes 12xxx \u00ab MELANGE \u2026 \u00bb, vrac + conditionn\u00e9s 250g). Sym\u00e9trique de int_oracle_lcdp__entree_fabrication_tasks (type 296, caf\u00e9 vert). Les deux flux NE partagent PAS de document parent (task_idtask nul) : pas de nomenclature/BOM ERP, donc pas de rattachement direct sortie\u2194intrants. Le rapprochement entr\u00e9e/sortie ne peut se faire qu'en agr\u00e9g\u00e9 (par p\u00e9riode), et les quantit\u00e9s m\u00e9langent des unit\u00e9s h\u00e9t\u00e9rog\u00e8nes (vrac en kg vs conditionn\u00e9s 250g) \u2192 un rendement de torr\u00e9faction n'est pas calculable en l'\u00e9tat sans normalisation. valuation bas\u00e9e sur purchase_unit_price, souvent absent pour les produits fabriqu\u00e9s (non achet\u00e9s) \u2192 peut \u00eatre nul. Aucun filtre statut ici.\n"""
    )
    as (
      

with sortie_fabrication_base as (

    select
        -- Identifiants
        thp.idtask_has_product as task_product_id,
        t.idtask as task_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idproduct_source as product_source_id,
        t.type_product_source as product_source_type,
        t.idproduct_destination as product_destination_id,
        t.type_product_destination as product_destination_type,

        -- Codes (source = FABRICATION, destination = dépôt torréfié : toujours COMPANY)
        cs.code as source_code,
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

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_product` as thp
        on t.idtask = thp.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product` as p
        on thp.idproduct = p.idproduct
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_status` as ts
        on t.idtask_status = ts.idtask_status

    -- Source = COMPANY (dépôt FABRICATION)
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as cs
        on
            t.idproduct_source = cs.idcompany
            and t.type_product_source = 'COMPANY'

    -- Destination = COMPANY (dépôt du café torréfié, typiquement TORREFIEVRAC)
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as cd
        on
            t.idproduct_destination = cd.idcompany
            and t.type_product_destination = 'COMPANY'

    where
        1 = 1
        and t.idtask_type = 297  -- SORTIE_FABRICATION
        and t.code_status_record = '1'
        and t.real_start_date is not null

    group by
        thp.idtask_has_product,
        t.idtask,
        t.idcompany_peer,
        thp.idproduct,
        t.idproduct_source,
        t.type_product_source,
        t.idproduct_destination,
        t.type_product_destination,
        cs.code, cd.code,
        p.code, ts.code,
        t.real_start_date,
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity,
        thp.net_price,
        p.purchase_unit_price,
        t.updated_at, t.created_at, t.extracted_at
)

select
    -- Identifiants
    task_product_id,
    task_id,
    company_id,
    product_id,
    product_source_id,
    product_source_type,
    product_destination_id,
    product_destination_type,

    -- Codes
    source_code,
    destination_code,
    product_code,
    task_status_code,

    -- Infos métier
    task_start_date,

    -- Infos de conditionnement
    unit_coeff_multi,
    unit_coeff_div,
    base_unit_quantity,
    product_unit_price_task,
    product_unit_price_latest,

    -- Métriques
    quantity,
    valuation,

    -- Timestamps techniques
    updated_at,
    created_at,
    extracted_at

from sortie_fabrication_base
where
    source_code is not null
    and destination_code is not null
    );
  