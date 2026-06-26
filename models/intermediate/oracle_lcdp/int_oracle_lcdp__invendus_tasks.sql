{{
    config(
        materialized='incremental',
        unique_key='task_product_id',
        partition_by={'field': 'task_start_date', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        cluster_by=['device_id', 'product_id'],
        description='Table intermédiaire des invendus LCDP (task_type 11) - produits constatés invendus et retirés des machines, au grain ligne produit'
    )
}}

with invendus_tasks as (

    select
        -- PK naturelle de task_has_product
        thp.idtask_has_product as task_product_id,

        -- IDs business
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idlocation as location_id,

        -- Codes métier pour les jointures futures
        c.code as company_code,
        d.code as device_code,
        p.code as product_code,
        ts.code as task_status_code,

        -- Infos métier
        l.access_info as task_location_info,
        t.real_start_date as task_start_date,

        -- Conditionnement & prix
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity as base_unit_quantity,
        thp.net_price as product_unit_price_task,
        p.purchase_unit_price as product_unit_price_latest,

        -- Métriques (quantité ramenée en unités de base)
        thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div as quantity,
        thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div * p.purchase_unit_price as valuation,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from {{ ref('stg_oracle_lcdp__task') }} as t
    inner join {{ ref('stg_oracle_lcdp__task_has_product') }} as thp on t.idtask = thp.idtask
    left join {{ ref('stg_oracle_lcdp__company') }} as c on t.idcompany_peer = c.idcompany
    left join {{ ref('stg_oracle_lcdp__device') }} as d on t.iddevice = d.iddevice
    left join {{ ref('stg_oracle_lcdp__product') }} as p on thp.idproduct = p.idproduct
    left join {{ ref('stg_oracle_lcdp__location') }} as l on t.idlocation = l.idlocation
    left join {{ ref('stg_oracle_lcdp__task_status') }} as ts on t.idtask_status = ts.idtask_status

    where
        1 = 1
        and t.idtask_status in (1, 4)  -- FAIT, VALIDE (un invendu ne compte que s'il est réalisé)
        and t.code_status_record = '1'
        and t.idtask_type = 11  -- INVENDUS
        and t.real_start_date is not null
)

select * from invendus_tasks

{% if is_incremental() %}
    where invendus_tasks.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from {{ this }} as t
    )
{% endif %}
