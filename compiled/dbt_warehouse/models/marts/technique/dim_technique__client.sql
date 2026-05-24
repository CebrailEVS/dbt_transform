

select
    client_id,
    partner_name,
    client_code,
    client_name,
    client_category,
    client_address,
    is_active as client_is_active,
    created_at,
    updated_at

from `evs-datastack-prod`.`prod_staging`.`stg_yuman__clients`