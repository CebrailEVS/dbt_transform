{{
    config(
        materialized='incremental',
        unique_key='task_product_id',
        partition_by={'field': 'task_start_date', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        cluster_by=['company_id', 'ca_category'],
        description='Lignes de facturation client (FACT CLIENT / AVOIR) avec leur coefficient de signe et leur catégorie de CA'
    )
}}

with coefficient_par_type as (

    -- Coefficient de signe du CA, paramétré dans l'ERP : 1 sur FACT CLIENT,
    -- -1 sur AVOIR. C'est la SEULE source de cette règle — sans elle un avoir
    -- s'ajouterait au chiffre d'affaires au lieu de s'en retrancher.
    -- `value` est un texte côté ERP, d'où le safe_cast.
    select
        idtask_type,
        safe_cast(value as float64) as sign_coefficient
    from {{ ref('stg_oracle_neshu__task_type_has_config') }}
    where idconfig = 'coefficient'
),

facturation_base as (

    select
        -- Identifiants
        thp.idtask_has_product as task_product_id,
        t.idtask as task_id,
        t.idcompany_peer as company_id,
        thp.idproduct as product_id,
        t.idtask_type as task_type_id,

        -- Codes
        c.code as company_code,
        c.name as company_name,
        p.code as product_code,
        ts.code as task_status_code,
        tt.code as task_type_code,

        -- Catégorie de CA. Les cinq branches sont mutuellement exclusives : les
        -- trois codes de prestation sont des produits de type 1 (PRODU), donc
        -- hors vending (type 3), et le `not in` du négoce les écarte.
        -- Une ligne qui ne relève d'aucune catégorie reste à NULL : le rapport
        -- Distrilog l'ignore, on la garde visible plutôt que de la faire
        -- disparaître (≈ 1 ligne sur 850 en août 2026).
        case
            when p.code = 'PRESTASERV' then 'PRESTA_SERVICE'
            when p.code = 'PRESTASERVFONT' then 'FONTAINES'
            when p.code = 'PRESTASERVAUUM' then 'LAVES_VERRES'
            when p.idproduct_type = 3 then 'VENDING'
            when p.idproduct_type = 1 then 'NEGOCE'
        end as ca_category,

        -- Informations métier
        t.real_start_date as task_start_date,

        -- Métriques
        thp.sale_amount_net,
        coalesce(cpt.sign_coefficient, 1) as sign_coefficient,
        thp.sale_amount_net * coalesce(cpt.sign_coefficient, 1) as ca_ht_eur,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from {{ ref('stg_oracle_neshu__task') }} as t
    inner join {{ ref('stg_oracle_neshu__task_has_product') }} as thp on t.idtask = thp.idtask
    inner join {{ ref('stg_oracle_neshu__product') }} as p on thp.idproduct = p.idproduct
    left join {{ ref('stg_oracle_neshu__company') }} as c on t.idcompany_peer = c.idcompany
    left join {{ ref('stg_oracle_neshu__task_status') }} as ts on t.idtask_status = ts.idtask_status
    left join {{ ref('stg_oracle_neshu__task_type') }} as tt on t.idtask_type = tt.idtask_type
    left join coefficient_par_type as cpt on t.idtask_type = cpt.idtask_type

    where
        1 = 1
        and t.idtask_status in (1, 2, 4)  -- FAIT, ENCOURS, VALIDE
        and t.code_status_record = '1'
        and t.idtask_type in (102, 106)  -- FACT CLIENT, AVOIR
        and t.real_start_date is not null
)

select
    -- Identifiants
    task_product_id,
    task_id,
    company_id,
    product_id,
    task_type_id,

    -- Codes
    company_code,
    company_name,
    product_code,
    task_status_code,
    task_type_code,
    ca_category,

    -- Infos métier
    task_start_date,

    -- Métriques
    sale_amount_net,
    sign_coefficient,
    ca_ht_eur,

    -- Timestamps techniques
    updated_at,
    created_at,
    extracted_at

from facturation_base

{% if is_incremental() %}
    where
        (
            updated_at > (
                select max(t.updated_at)
                from {{ this }} as t
            )
            or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
        )
{% endif %}
