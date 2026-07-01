{{ config(materialized='table') }}

with delivery_status as (

    -- Pivot du label au grain tâche (1 label STATUT_LIVRAISON par tâche aujourd'hui).
    -- Pivot plutôt que jointure + dédup : pas de fan-out sur les lignes produit,
    -- robuste si une seconde famille de label apparaît un jour.
    select
        lht.idtask,
        max(case when lf.code = 'STATUT_LIVRAISON' then la.code end) as delivery_status_code

    from {{ ref('stg_oracle_neshu__label_has_task') }} as lht
    inner join {{ ref('stg_oracle_neshu__label') }} as la
        on lht.idlabel = la.idlabel
    inner join {{ ref('stg_oracle_neshu__label_family') }} as lf
        on la.idlabel_family = lf.idlabel_family

    group by lht.idtask
),

commande_fournisseur_base as (

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

        -- Codes (source = fournisseur, destination = dépôt : toujours COMPANY)
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

    from {{ ref('stg_oracle_neshu__task') }} as t
    inner join {{ ref('stg_oracle_neshu__task_has_product') }} as thp
        on t.idtask = thp.idtask
    left join {{ ref('stg_oracle_neshu__product') }} as p
        on thp.idproduct = p.idproduct
    left join {{ ref('stg_oracle_neshu__task_status') }} as ts
        on t.idtask_status = ts.idtask_status

    -- Source = COMPANY (fournisseur)
    left join {{ ref('stg_oracle_neshu__company') }} as cs
        on
            t.idproduct_source = cs.idcompany
            and t.type_product_source = 'COMPANY'

    -- Destination = COMPANY (dépôt)
    left join {{ ref('stg_oracle_neshu__company') }} as cd
        on
            t.idproduct_destination = cd.idcompany
            and t.type_product_destination = 'COMPANY'

    where
        1 = 1
        and t.idtask_type = 120  -- COMMANDE FOURNISSEUR
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
    cfb.task_product_id,
    cfb.task_id,
    cfb.company_id,
    cfb.product_id,
    cfb.product_source_id,
    cfb.product_source_type,
    cfb.product_destination_id,
    cfb.product_destination_type,

    -- Codes
    cfb.source_code,
    cfb.destination_code,
    cfb.product_code,
    cfb.task_status_code,
    ds.delivery_status_code,

    -- Infos métier
    cfb.task_start_date,

    -- Infos de conditionnement
    cfb.unit_coeff_multi,
    cfb.unit_coeff_div,
    cfb.base_unit_quantity,
    cfb.product_unit_price_task,
    cfb.product_unit_price_latest,

    -- Métriques
    cfb.quantity,
    cfb.valuation,

    -- Timestamps techniques
    cfb.updated_at,
    cfb.created_at,
    cfb.extracted_at

from commande_fournisseur_base as cfb
left join delivery_status as ds
    on cfb.task_id = ds.idtask
where
    cfb.source_code is not null
    and cfb.destination_code is not null
