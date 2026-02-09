{{ config(
    materialized='table',
    description='Table intermédiaire des mouvements internes (livraisons internes) avec source et destination enrichies. \
    Dédupliquée sur task_product_id avec priorité au label_code = LIVRE.'
) }}

with commande_interne_base as (

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
        la.code as label_code,

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

    from {{ ref('stg_oracle_neshu__task') }} as t
    inner join {{ ref('stg_oracle_neshu__task_has_product') }} as thp
        on t.idtask = thp.idtask
    left join {{ ref('stg_oracle_neshu__product') }} as p
        on thp.idproduct = p.idproduct
    left join {{ ref('stg_oracle_neshu__task_status') }} as ts
        on t.idtask_status = ts.idtask_status

    -- Source = COMPANY
    left join {{ ref('stg_oracle_neshu__company') }} as cs
        on
            t.idproduct_source = cs.idcompany
            and t.type_product_source = 'COMPANY'

    -- Source = RESOURCES
    left join {{ ref('stg_oracle_neshu__resources') }} as rs
        on
            t.idproduct_source = rs.idresources
            and t.type_product_source = 'RESOURCES'

    -- Destination = COMPANY
    left join {{ ref('stg_oracle_neshu__company') }} as cd
        on
            t.idproduct_destination = cd.idcompany
            and t.type_product_destination = 'COMPANY'

    -- Destination = RESOURCES
    left join {{ ref('stg_oracle_neshu__resources') }} as rd
        on
            t.idproduct_destination = rd.idresources
            and t.type_product_destination = 'RESOURCES'

    -- Jointures pour le filtrage sur labels télémétrie
    left join {{ ref('stg_oracle_neshu__label_has_task') }} as lht
        on t.idtask = lht.idtask
    left join {{ ref('stg_oracle_neshu__label') }} as la
        on lht.idlabel = la.idlabel
    left join {{ ref('stg_oracle_neshu__label_family') }} as lf
        on la.idlabel_family = lf.idlabel_family

    where
        1 = 1
        and t.idtask_status in (1, 4, 3)  -- FAIT, VALIDE, ANNULE
        and t.code_status_record = '1'
        and t.idtask_type = 132  -- LIVRAISON INTERNE
        and lf.code = 'STATUT_LIVRAISON'
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
        cs.code, rs.code, cd.code, rd.code,
        p.code, ts.code, la.code,
        t.real_start_date,
        thp.unit_coeff_multi,
        thp.unit_coeff_div,
        thp.real_quantity,
        thp.net_price,
        p.purchase_unit_price,
        t.updated_at, t.created_at, t.extracted_at
),

dedup as (
    select
        *,
        row_number() over (
            partition by task_product_id
            order by
                case when label_code = 'LIVRE' then 1 else 2 end
        ) as rn
    from commande_interne_base
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
    label_code,

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

from dedup
where
    rn = 1
    and source_code is not null
    and destination_code is not null
