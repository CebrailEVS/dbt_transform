

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_workorder_demands`

),

cleaned_workorder_demands as (

    select
        id as demand_id,
        workorder_id,
        material_id,
        site_id,
        client_id,
        user_id,
        cast(contact_id as int64) as contact_id,
        category_id as demand_category_id,
        description as demand_description,
        status as demand_status,
        reject_comment as demand_reject_comment,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data
    where id is not null

)

select *
from cleaned_workorder_demands