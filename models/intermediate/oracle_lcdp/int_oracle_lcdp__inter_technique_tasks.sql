{{
    config(
        materialized='incremental',
        unique_key='task_id',
        partition_by={'field': 'task_start_date', 'data_type': 'timestamp'},
        incremental_strategy='merge',
        description='Table des interventions techniques (task_type 131) avec labels task pivotés'
    )
}}

with base_task as (

    select
        -- Identifiants
        t.idtask as task_id,
        t.iddevice as device_id,
        t.idcompany_peer as company_id,
        thr.idresources as resources_id,

        -- Codes
        c.code as company_code,
        d.code as device_code,

        -- Noms
        c.name as company_name,
        d.name as device_name,

        -- Infos métier
        l.access_info as task_location_info,
        t.comments_self,
        t.comments_peer,
        t.real_start_date as task_start_date,
        t.real_end_date as task_end_date,

        -- Status
        ts.code as task_status_code,

        -- Timestamps techniques
        t.updated_at,
        t.created_at,
        t.extracted_at

    from {{ ref('stg_oracle_lcdp__task') }} as t
    left join {{ ref('stg_oracle_lcdp__company') }} as c
        on t.idcompany_peer = c.idcompany
    left join {{ ref('stg_oracle_lcdp__device') }} as d
        on t.iddevice = d.iddevice
    left join {{ ref('stg_oracle_lcdp__task_has_resources') }} as thr
        on t.idtask = thr.idtask
    left join {{ ref('stg_oracle_lcdp__resources') }} as r
        on
            thr.idresources = r.idresources
            and r.idresources_type = 2
    left join {{ ref('stg_oracle_lcdp__task_status') }} as ts
        on t.idtask_status = ts.idtask_status
    left join {{ ref('stg_oracle_lcdp__location') }} as l
        on t.idlocation = l.idlocation

    where
        1 = 1
        and t.idtask_type = 131
        and t.code_status_record = '1'
        and t.real_start_date is not null
        and r.idresources_type = 2 -- Ensure we only get resources type = people

        {% if is_incremental() %}
            and t.updated_at >= (
                select max(src.updated_at) - interval 1 day
                from {{ this }} as src
            )
        {% endif %}

),

label_pivot as (

    select
        t.idtask as task_id,
        max(
            case
                when lf.code = 'Statut inter'
                    then la.code
            end
        ) as statut_inter,
        max(
            case
                when lf.code = 'Objet intervent'
                    then la.code
            end
        ) as objet_intervent,
        max(
            case
                when lf.code = 'DEVICE_CANCEL_REASON'
                    then la.code
            end
        ) as device_cancel_reason

    from {{ ref('stg_oracle_lcdp__task') }} as t
    left join {{ ref('stg_oracle_lcdp__label_has_task') }} as lht
        on t.idtask = lht.idtask
    left join {{ ref('stg_oracle_lcdp__label') }} as la
        on lht.idlabel = la.idlabel
    left join {{ ref('stg_oracle_lcdp__label_family') }} as lf
        on la.idlabel_family = lf.idlabel_family

    where
        1 = 1

    group by
        t.idtask

),

deduped_task as (

    select *
    from (
        select
            bt.*,
            row_number() over (
                partition by bt.task_id
                order by bt.resources_id
            ) as rn
        from base_task as bt
    )
    where rn = 1

)

select
    -- Identifiants
    bt.task_id,
    bt.device_id,
    bt.company_id,
    bt.resources_id,

    -- Codes
    bt.company_code,
    bt.device_code,

    -- Noms
    bt.company_name,
    bt.device_name,

    -- Infos métier
    bt.task_location_info,
    bt.comments_self,
    bt.comments_peer,
    bt.task_start_date,
    bt.task_end_date,
    bt.task_status_code,

    -- Labels pivotés
    lp.statut_inter,
    lp.objet_intervent,
    lp.device_cancel_reason,

    -- Timestamps techniques
    bt.updated_at,
    bt.created_at,
    bt.extracted_at

from deduped_task as bt
left join label_pivot as lp
    on bt.task_id = lp.task_id
