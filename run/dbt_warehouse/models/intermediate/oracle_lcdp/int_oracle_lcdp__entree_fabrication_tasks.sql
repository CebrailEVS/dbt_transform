
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__entree_fabrication_tasks`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Entr\u00e9es en fabrication LCDP (type 296) : une ligne par produit de caf\u00e9 vert consomm\u00e9 par la torr\u00e9faction. C'est l'intrant du process \u2014 le caf\u00e9 vert sort du d\u00e9p\u00f4t de stockage (source, typiquement VERT) pour entrer dans le d\u00e9p\u00f4t FABRICATION (destination).\n[COMMENT CONSTRUITE] stg_oracle_lcdp__task (idtask_type = 296, code_status_record = '1', real_start_date non nul, tous statuts conserv\u00e9s) crois\u00e9 avec task_has_product (grain ligne produit), enrichi product et task_status. Source et destination r\u00e9solues sur company (COMPANY dans 100 % des cas). Seules les lignes dont source ET destination se r\u00e9solvent sont conserv\u00e9es. Quantit\u00e9 ramen\u00e9e en unit\u00e9s de base (real_quantity \u00d7 coeff_multi / coeff_div).\n[GRAIN] 1 ligne par task_product_id (ligne produit d'une entr\u00e9e en fabrication). ~1 380 lignes (~208 t\u00e2ches, 39 caf\u00e9s verts), depuis janvier 2025.\n[NOTES] Maillon amont de la cha\u00eene de torr\u00e9faction : VERT (caf\u00e9 vert, codes 11xxx \u00ab CV \u2026 \u00bb) \u2192 [ENTREE_FABRICATION] \u2192 FABRICATION \u2192 [SORTIE_FABRICATION] \u2192 TORREFIEVRAC. Sym\u00e9trique de int_oracle_lcdp__sortie_fabrication_tasks (type 297, produits torr\u00e9fi\u00e9s 12xxx). Les deux flux NE partagent PAS de document parent (task_idtask nul) : impossible de rattacher nativement un lot de sortie \u00e0 ses intrants (pas de nomenclature/BOM dans l'ERP). Aucun filtre statut ici (tri en marts). Explique le stock de caf\u00e9 vert du d\u00e9p\u00f4t VERT re\u00e7u sans commande fournisseur.\n"""
    )
    as (
      

with entree_fabrication_base as (

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

        -- Codes (source = dépôt café vert, destination = FABRICATION : toujours COMPANY)
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

    -- Source = COMPANY (dépôt d'où sort le café vert, typiquement VERT)
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as cs
        on
            t.idproduct_source = cs.idcompany
            and t.type_product_source = 'COMPANY'

    -- Destination = COMPANY (dépôt FABRICATION)
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as cd
        on
            t.idproduct_destination = cd.idcompany
            and t.type_product_destination = 'COMPANY'

    where
        1 = 1
        and t.idtask_type = 296  -- ENTREE_FABRICATION
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

from entree_fabrication_base
where
    source_code is not null
    and destination_code is not null
    );
  