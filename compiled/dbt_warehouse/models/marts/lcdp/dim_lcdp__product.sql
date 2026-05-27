

with product_labels as (
    select
        p.idproduct as product_id,
        p.idproduct_type as product_type_id,
        p.code as product_code,
        p.name as product_name,
        p.purchase_unit_price,
        -- Correction de created_at si idproduct = 1
        p.updated_at,
        lp.label_code,
        lp.label_text_fr,
        lp.label_family_code,
        case
            when p.idproduct = 1 and p.created_at is null then p.updated_at
            else p.created_at
        end as created_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product` as p
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_product` as lp
        on p.idproduct = lp.product_id
    where
        p.idproduct_type in (1, 5)
        and (
            case
                when p.idproduct = 1 and p.created_at is null then p.updated_at
                else p.created_at
            end
        ) is not null
),

aggregated_labels as (
    select
        product_id,
        product_type_id,
        product_code,
        product_name,
        purchase_unit_price,
        created_at,
        updated_at,
        MAX(case when label_family_code = 'FAMIPRO' then label_text_fr end) as product_family,
        MAX(case when label_family_code = 'GROUPRO' then label_text_fr end) as product_group,
        MAX(case when label_family_code = 'BIO' then label_text_fr end) as product_bio,
        -- is_active conservé sur le code (YES/NO) pour le test lower(...) = 'yes' ci-dessous
        MAX(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        MAX(case when label_family_code = 'MARQPRO' then label_text_fr end) as product_brand
    from product_labels
    group by
        product_id,
        product_type_id,
        product_code,
        product_name,
        purchase_unit_price,
        created_at,
        updated_at
)

select
    -- Identifiants
    product_id,
    product_type_id,

    -- Codes et noms
    product_code,
    product_name,
    purchase_unit_price,

    -- Caractéristiques produit
    product_family,
    product_group,
    product_brand,
    product_bio,
    COALESCE(LOWER(is_active) = 'yes', false) as is_active,

    -- Dates
    created_at,
    updated_at

from aggregated_labels