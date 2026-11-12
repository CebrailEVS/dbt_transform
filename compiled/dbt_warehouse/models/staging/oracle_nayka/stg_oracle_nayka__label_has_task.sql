

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_label_has_task`
),

cleaned_data as (
    select
        cast(idtask as int64) as idtask,
        cast(idlabel as int64) as idlabel,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data