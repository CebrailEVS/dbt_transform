{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_task_has_resources') }}
),

cleaned_data as (
    select
        cast(idtask as int64) as idtask,
        cast(idresources as int64) as idresources,
        cast(task_status as int64) as task_status,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
