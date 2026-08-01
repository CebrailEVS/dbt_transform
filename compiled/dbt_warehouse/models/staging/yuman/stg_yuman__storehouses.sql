

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_evs_products_storehouses`

),

cleaned_storehouses as (

    select
        id as storehouses_id,
        name as storehouses_name,
        address as storehouses_address,
        _extracted_at as extracted_at
    from source_data

)

select *
from cleaned_storehouses