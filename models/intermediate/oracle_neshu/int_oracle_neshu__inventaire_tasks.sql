{{
    config(
        materialized='table',
        cluster_by=['company_id', 'product_id','task_status_code'],
        description='Table intermédiaire des tâches d"inventaire - avec enrichissement produit, client, quantité et valorisation'
    )
}}

with inventaire_base as (
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
        -- Métriques
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) as quantity,
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div * thp.net_price) as valuation,
        -- Timestamps techniques
        t.updated_at,
        t.created_at
    from {{ ref('stg_oracle_neshu__task') }} t
    inner join {{ ref('stg_oracle_neshu__task_has_product') }} thp on thp.idtask = t.idtask
    left join {{ ref('stg_oracle_neshu__product') }} p on p.idproduct = thp.idproduct
    left join {{ ref('stg_oracle_neshu__task_status') }} ts on t.idtask_status = ts.idtask_status
    -- jointure company pour la source si COMPANY
    left join {{ ref('stg_oracle_neshu__company') }} cs 
        on t.IDPRODUCT_SOURCE = cs.idcompany 
       and t.type_product_source = 'COMPANY'
    -- jointure resources pour la source si RESOURCES
    left join {{ ref('stg_oracle_neshu__resources') }} r 
        on t.IDPRODUCT_SOURCE = r.idresources
       and t.type_product_source = 'RESOURCES'
    where 1=1
        and t.idtask_status in (1, 4, 3)  -- FAIT, VALIDE, ANNULE
        and t.code_status_record = '1'
        and t.idtask_type = 162 -- INVENTAIRE
        and t.real_start_date is not null
    group by
        thp.idtask_has_product, t.idtask, t.idcompany_peer,
        t.idproduct_source, t.type_product_source,
        thp.idproduct, thp.net_price,
        cs.code, r.code, p.code, ts.code,
        t.real_start_date,
        t.updated_at, t.created_at
)
select *
from inventaire_base
WHERE source_code IS not null
