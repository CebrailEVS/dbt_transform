{{
    config(
        materialized='incremental',
        unique_key='task_id',
        partition_by={'field': 'task_start_date', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        cluster_by=['company_id', 'device_id', 'task_status_code'],
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

    from {{ ref('stg_oracle_neshu__task') }} t
    left join {{ ref('stg_oracle_neshu__company') }} c on c.idcompany = t.idcompany_peer
    left join {{ ref('stg_oracle_neshu__task_status') }} ts on t.idtask_status = ts.idtask_status
    left join {{ ref('stg_oracle_neshu__location') }} l on l.idlocation = t.idlocation

    where 1=1
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
  where updated_at >= (
      select max(updated_at) - interval 1 day
      from {{ this }}
  )
{% endif %}