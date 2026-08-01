

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_evs_products`

),

cleaned_products as (

    select
        id as product_id,
        reference as product_code,
        designation as product_name,
        product_type,
        brand as product_brand,
        unit as product_unit,
        purchase_price as product_purchase_price,
        sale_price as product_sale_price,
        active as is_active,
        coalesce(lower((
            select json_value(field, '$.value')
            from unnest(json_query_array(_embed, '$.fields')) as field
            where json_value(field, '$.name') = 'ARTICLE INTERDIT'
        )) in ('oui', 'yes', 'true'), false) as is_forbidden_article,
        coalesce(lower((
            select json_value(field, '$.value')
            from unnest(json_query_array(_embed, '$.fields')) as field
            where json_value(field, '$.name') = 'ARTICLE OBLIGATOIRE'
        )) in ('oui', 'yes', 'true'), false) as is_mandatory_article,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        _extracted_at as extracted_at
    from source_data
    where id is not null

)

select *
from cleaned_products