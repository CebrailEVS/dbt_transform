
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_lcdp__product`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Dimension produit LCDP enrichie des labels m\u00e9tier (famille, groupe, marque, bio).\n[COMMENT CONSTRUITE] Issu de stg_oracle_lcdp__product filtr\u00e9 sur product_type_id IN (1, 5), enrichi par pivot des labels via stg_oracle_lcdp__label_has_product : FAMIPRO, GROUPRO, MARQPRO, BIO, ISACTIVE. created_at corrig\u00e9 si NULL pour idproduct=1 (fallback updated_at).\n[GRAIN] 1 ligne par product_id.\n[NOTES] product_type_id : 1 = produit, 5 = ligne de prix. is_active converti en bool\u00e9en.\n"""
    )
    as (
      

with product_labels as (
    select
        p.idproduct as product_id,
        p.idproduct_type as product_type_id,
        p.code as product_code,
        p.name as product_name,
        p.purchase_unit_price,
        -- Correction de created_at si idproduct = 1
        p.updated_at,
        l.code as label_code,
        lf.code as label_family_code,
        case
            when p.idproduct = 1 and p.created_at is null then p.updated_at
            else p.created_at
        end as created_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product` as p
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_product` as lhp
        on
            p.idproduct = lhp.idproduct
            and lhp.idlabel is not null
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as l
        on lhp.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
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
        MAX(case when label_family_code = 'FAMIPRO' then label_code end) as product_family,
        MAX(case when label_family_code = 'GROUPRO' then label_code end) as product_group,
        MAX(case when label_family_code = 'BIO' then label_code end) as product_bio,
        MAX(case when label_family_code = 'ISACTIVE' then label_code end) as is_active,
        MAX(case when label_family_code = 'MARQPRO' then label_code end) as product_brand
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
    );
  