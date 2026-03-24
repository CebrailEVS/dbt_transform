
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__product`
      
    
    

    
    OPTIONS(
      description="""Dimension produit enrichie \u00e0 partir des labels associ\u00e9s (type, famille, groupe, marque, etc.) et filtr\u00e9e sur les produits de type 1 (produit) et 5 (ligne de prix).\n"""
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
        case
            when p.idproduct = 1 and p.created_at is null then p.updated_at
            else p.created_at
        end as created_at,
        p.updated_at,
        l.code as label_code,
        lf.code as label_family_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as p
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_product` as lhp
        on
            p.idproduct = lhp.idproduct
            and lhp.idlabel is not null
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhp.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
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

pivoted as (
    select *
    from product_labels
    pivot (
        MAX(label_code)
        for label_family_code in (
            'MARQUEP' as product_brand,
            'PROPRIETAIRE' as product_owner,
            'FAMILLE' as product_family,
            'BIO' as product_bio,
            'PLANOETE' as product_planoete,
            'PLANOHIVER' as product_planohiver,
            'HPALME' as product_hpalme,
            'CLASSABC' as product_classabc,
            'EXPLOIT' as product_exploit,
            'GROUPE' as product_group,
            'LPTYPE' as product_type_raw,
            'ISACTIVE' as isactive
        )
    )
),

final as (
    select
        product_id,
        product_type_id,
        product_code,
        product_name,
        purchase_unit_price,
        product_brand,
        product_owner,
        product_family,
        product_bio,
        product_planoete,
        product_planohiver,
        product_hpalme,
        product_classabc,
        product_exploit,
        product_group,
        product_type_raw,
        -- convert isactive from string to boolean
        COALESCE(LOWER(isactive) = 'yes', false) as is_active,
        created_at,
        updated_at,
        -- logique de typologie standardisée
        COALESCE(
            case
                when product_id = 1 then 'INDEFINI'
                when product_family in ('CAFE CAPSULES', 'CAFE CAPSULES PREMIUM') then 'CAFE CAPS'
                when product_family in ('THE') then 'THE'
                when product_group = 'ACCESSOIRES' then 'ACCESSOIRES'
                when product_group = 'BOISSONS FRAICHES' then 'BOISSONS FRAICHES'
                when product_group = 'SNACKING' then 'SNACKING'
                when product_code = 'VANHCHOC23' then 'CHOCOLATS VAN HOUTEN'
                when product_type_raw = 'BGOURMANDE' then 'BOISSONS GOURMANDES'
            end,
            NULLIF(TRIM(product_type_raw), ''),
            'Non renseigné'
        ) as product_type
    from pivoted
),

standardized as (
    select
        product_id,
        product_type_id,
        product_code,
        product_name,
        purchase_unit_price,

        -- FORCING champs à 'INDEFINI' si product_type est 'INDEFINI'
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_brand end as product_brand,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_owner end as product_owner,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_family end as product_family,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_bio end as product_bio,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_planoete end as product_planoete,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_planohiver end as product_planohiver,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_hpalme end as product_hpalme,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_classabc end as product_classabc,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_exploit end as product_exploit,
        case when product_type = 'INDEFINI' then 'INDEFINI' else product_group end as product_group,

        product_type_raw,
        is_active,
        created_at,
        updated_at,
        product_type
    from final
)

select
    product_id,
    product_type_id,
    product_code,
    product_name,
    purchase_unit_price,
    product_brand,
    product_owner,
    product_family,
    product_bio,
    product_planoete,
    product_planohiver,
    product_hpalme,
    product_classabc,
    product_exploit,
    product_group,
    product_type,
    is_active,
    created_at,
    updated_at
from standardized
    );
  