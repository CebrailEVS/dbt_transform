

with ecart_inventaire_base as (

    select
        -- Identifiants
        thp.idtask_has_product as task_product_id,
        t.idtask as task_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idproduct_source as product_source_id,
        t.type_product_source as product_source_type,

        -- Codes
        case 
            when t.type_product_source = 'COMPANY' then cs.code
            when t.type_product_source = 'RESOURCES' then r.code
        end as source_code,
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

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` thp on thp.idtask = t.idtask
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` p on p.idproduct = thp.idproduct
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` ts on t.idtask_status = ts.idtask_status

    -- jointure company pour la source si COMPANY
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` cs 
        on t.idproduct_source = cs.idcompany 
       and t.type_product_source = 'COMPANY'

    -- jointure resources pour la source si RESOURCES
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` r 
        on t.idproduct_source = r.idresources
       and t.type_product_source = 'RESOURCES'

    where 1=1
        and t.idtask_status in (1, 4, 3)  -- FAIT, VALIDE, ANNULE
        and t.code_status_record = '1'
        and t.idtask_type = 163 -- ECART INVENTAIRE
        and t.real_start_date is not null

    group by
        thp.idtask_has_product, t.idtask, t.idcompany_peer,
        t.idproduct_source, t.type_product_source,
        thp.idproduct,
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity,
        thp.net_price,
        p.purchase_unit_price,
        cs.code, r.code, p.code, ts.code,
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

    -- Codes
    source_code,
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

from ecart_inventaire_base
where source_code is not null