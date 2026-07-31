{{
    config(
        materialized='table',
        description='label_has_task nettoyés et enrichis depuis evs_label_has_task'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_label_has_task') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idtask as int64) as idtask,

        -- Timestamps harmonisés
        timestamp(_extracted_at) as extracted_at

    from source_data
),

filtered_data as (
    select lht.*
    from cleaned_data as lht
    inner join {{ ref('stg_oracle_neshu__task') }} as t
        on lht.idtask = t.idtask
)

select * from filtered_data
