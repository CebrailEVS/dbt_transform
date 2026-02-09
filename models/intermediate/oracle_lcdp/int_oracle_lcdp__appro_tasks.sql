{{
    config(
        materialized='incremental',
        unique_key='task_id',
        partition_by={'field': 'task_start_date', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        description='Table intermédiaire des passages approvisionneurs - avec enrichissement'
    )
}}

with passage_appro_base as (

    select
        -- Identifiants
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        t.idproduct_source as product_source_id,
        t.type_product_source as product_source_type,
        t.idproduct_destination as product_destination_id,
        t.type_product_destination as product_destination_type,

        -- Codes
        c.code as company_code,

        -- Infos métier
        l.access_info as task_location_info,
        t.real_start_date as task_start_date,
        t.real_end_date as task_end_date,

        -- Status
        ts.code as task_status_code,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from {{ ref('stg_oracle_lcdp__task') }} as t
    left join {{ ref('stg_oracle_lcdp__company') }} as c on t.idcompany_peer = c.idcompany
    left join {{ ref('stg_oracle_lcdp__task_status') }} as ts on t.idtask_status = ts.idtask_status
    left join {{ ref('stg_oracle_lcdp__location') }} as l on t.idlocation = l.idlocation

    where
        1 = 1
        and t.idtask_type = 32 -- PASSAGE APPROVISIONNEURS
        and t.code_status_record = '1'
        and t.real_start_date is not null

    group by
        t.idtask, t.iddevice, t.idcompany_peer,
        t.idproduct_source, t.type_product_source,
        t.idproduct_destination, t.type_product_destination,
        ts.code, c.code, l.access_info,
        t.real_start_date, t.real_end_date,
        t.updated_at, t.created_at, t.extracted_at
)

select
    -- Identifiants
    task_id,
    device_id,
    company_id,
    product_source_id,
    product_destination_id,

    -- Codes
    company_code,

    -- Infos métier
    product_source_type,
    product_destination_type,
    task_location_info,
    task_status_code,
    task_start_date,
    task_end_date,

    -- Timestamps techniques
    updated_at,
    created_at,
    extracted_at

from passage_appro_base

{% if is_incremental() %}
    where passage_appro_base.updated_at >= (
        select max(t.updated_at) - interval 1 day
        from {{ this }} as t
    )
{% endif %}
