

select
    ym.material_id,
    ym.site_id,
    ym.material_description,
    ym.material_name,
    ym.material_brand,
    ym.material_serial_number,
    ycat.category_name,
    ym.material_in_service_date,
    ym.created_at,
    ym.updated_at

from `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials` as ym
left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories` as ycat
    on ym.category_id = ycat.category_id