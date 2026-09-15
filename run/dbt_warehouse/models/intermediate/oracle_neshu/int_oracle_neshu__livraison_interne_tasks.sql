
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_interne_tasks`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Livraisons internes NESHU : une ligne par produit d\u00e9plac\u00e9 lors d'un mouvement de stock interne (typiquement d\u00e9p\u00f4t \u2192 v\u00e9hicule pour approvisionner les roadmen). Suivi des quantit\u00e9s et valeurs transf\u00e9r\u00e9es entre entit\u00e9s internes.\n[COMMENT CONSTRUITE] stg_oracle_neshu__task (idtask_type = 161 LIVRAISONS INTERNE, statuts FAIT/VALIDE/ANNULE/ ANOMALIE, code_status_record = '1') crois\u00e9 avec task_has_product (grain ligne produit), enrichi product. Source ET destination r\u00e9solues selon leur type (COMPANY \u2192 company, RESOURCES \u2192 resources). Quantit\u00e9 ramen\u00e9e en unit\u00e9s de base.\n[GRAIN] 1 ligne par task_product_id (ligne produit d'un mouvement interne). ~94k lignes (~9 048 t\u00e2ches), depuis d\u00e9but 2023.\n[NOTES] Seules les lignes dont la source ET la destination se r\u00e9solvent sont conserv\u00e9es. En pratique le flux dominant est COMPANY (d\u00e9p\u00f4t) \u2192 RESOURCES (v\u00e9hicule/roadman).\n"""
    )
    as (
      

with mouvement_interne_base as (

    select
        -- Identifiants
        thp.idtask_has_product as task_product_id,
        t.idtask as task_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idproduct_source as product_source_id,
        t.idproduct_destination as product_destination_id,
        t.type_product_source as product_source_type,
        t.type_product_destination as product_destination_type,

        -- Codes (source et destination)
        case
            when t.type_product_source = 'COMPANY' then cs.code
            when t.type_product_source = 'RESOURCES' then rs.code
        end as source_code,

        case
            when t.type_product_destination = 'COMPANY' then cd.code
            when t.type_product_destination = 'RESOURCES' then rd.code
        end as destination_code,

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

    -- Source = COMPANY
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as cs
        on
            t.idproduct_source = cs.idcompany
            and t.type_product_source = 'COMPANY'

    -- Source = RESOURCES
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as rs
        on
            t.idproduct_source = rs.idresources
            and t.type_product_source = 'RESOURCES'

    -- Destination = COMPANY
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as cd
        on
            t.idproduct_destination = cd.idcompany
            and t.type_product_destination = 'COMPANY'

    -- Destination = RESOURCES
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as rd
        on
            t.idproduct_destination = rd.idresources
            and t.type_product_destination = 'RESOURCES'

    where
        1 = 1
        and t.idtask_status in (1, 4, 3, 5)  -- FAIT, VALIDE, ANNULE, ANOMALIE
        and t.code_status_record = '1'
        and t.idtask_type = 161 -- LIVRAISONS INTERNE
        and t.real_start_date is not null

    group by
        thp.idtask_has_product, t.idtask, t.idcompany_peer,
        t.idproduct_source, t.type_product_source,
        t.idproduct_destination, t.type_product_destination,
        thp.idproduct,
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity,
        thp.net_price,
        p.purchase_unit_price,
        cs.code, rs.code, cd.code, rd.code,
        p.code, ts.code,
        t.real_start_date,
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

from mouvement_interne_base
where
    source_code is not null
    and destination_code is not null
    );
  