{{
    config(
        materialized='table',
        description='Table intermédiaire des tâches de livraison - avec enrichissement produit, client, quantité et valorisation'
    )
}}

with livraison_base as (

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

        -- Codes
        c.code as company_code,
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

    from {{ ref('stg_oracle_lcdp__task') }} as t
    inner join {{ ref('stg_oracle_lcdp__task_has_product') }} as thp on t.idtask = thp.idtask
    left join {{ ref('stg_oracle_lcdp__company') }} as c on t.idcompany_peer = c.idcompany
    left join {{ ref('stg_oracle_lcdp__product') }} as p on thp.idproduct = p.idproduct
    left join {{ ref('stg_oracle_lcdp__task_status') }} as ts on t.idtask_status = ts.idtask_status

    where
        1 = 1
        and t.idtask_status in (1, 4, 3)  -- FAIT, VALIDE, ANNULE
        and t.code_status_record = '1'
        and t.idtask_type = 101 -- BON LIVRAISON
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
        c.code, p.code,
        ts.code, t.real_start_date,
        t.updated_at, t.created_at, t.extracted_at
)

select
    -- Identifiants
    task_product_id,
    task_id,
    company_id,
    product_id,
    product_source_id,
    product_destination_id,
    product_source_type,
    product_destination_type,

    -- Codes
    company_code,
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

from livraison_base
