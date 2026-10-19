{{ config(materialized='table') }}

-- Référentiel des articles Neshu pour Cockpit Supply, construit depuis le staging.
-- Périmètre : produits de type 1 et 5, hors ghost-deletes ERP, ayant une date de création (celle du
-- produit 1 est reprise de sa modification).

with labels as (
    select
        lhp.idproduct,
        max(case when lf.code = 'FAMILLE' then l.code end) as product_family,
        max(case when lf.code = 'BIO' then l.code end) as product_bio,
        max(case when lf.code = 'PLANOETE' then l.code end) as product_planoete,
        max(case when lf.code = 'PLANOHIVER' then l.code end) as product_planohiver,
        max(case when lf.code = 'HPALME' then l.code end) as product_hpalme,
        max(case when lf.code = 'CLASSABC' then l.code end) as product_classabc,
        max(case when lf.code = 'EXPLOIT' then l.code end) as product_exploit,
        max(case when lf.code = 'GROUPE' then l.code end) as product_group,
        max(case when lf.code = 'LPTYPE' then l.code end) as product_type_raw,
        max(case when lf.code = 'ISACTIVE' then l.code end) as is_active
    from {{ ref('stg_oracle_neshu__label_has_product') }} as lhp
    inner join {{ ref('stg_oracle_neshu__label') }} as l
        on lhp.idlabel = l.idlabel
    inner join {{ ref('stg_oracle_neshu__label_family') }} as lf
        on l.idlabel_family = lf.idlabel_family
    group by lhp.idproduct
),

typed as (
    select
        p.idproduct as product_id,
        p.code as product_code,
        p.name as product_name,
        p.purchase_unit_price,
        lb.product_family,
        lb.product_bio,
        lb.product_planoete,
        lb.product_planohiver,
        lb.product_hpalme,
        lb.product_classabc,
        lb.product_exploit,
        lb.product_group,
        coalesce(lower(lb.is_active) = 'yes', false) as is_active,
        -- Typologie standardisée de l'article ; le produit 1 est un produit technique « indéfini ».
        coalesce(
            case
                when p.idproduct = 1 then 'INDEFINI'
                when lb.product_family in ('CAFE CAPSULES', 'CAFE CAPSULES PREMIUM') then 'CAFE CAPS'
                when lb.product_family in ('THE') then 'THE'
                when lb.product_group = 'ACCESSOIRES' then 'ACCESSOIRES'
                when lb.product_group = 'BOISSONS FRAICHES' then 'BOISSONS FRAICHES'
                when lb.product_group = 'SNACKING' then 'SNACKING'
                when p.code = 'VANHCHOC23' then 'CHOCOLATS VAN HOUTEN'
                when lb.product_type_raw = 'BGOURMANDE' then 'BOISSONS GOURMANDES'
            end,
            nullif(trim(lb.product_type_raw), ''),
            'Non renseigné'
        ) as product_type
    from {{ ref('stg_oracle_neshu__product') }} as p
    left join labels as lb
        on p.idproduct = lb.idproduct
    where
        p.idproduct_type in (1, 5)
        and p.code_status_record <> -1
        and (case when p.idproduct = 1 and p.created_at is null then p.updated_at else p.created_at end) is not null
)

-- Un article « indéfini » a tous ses attributs forcés à INDEFINI.
select
    product_id,
    product_code,
    product_name,
    if(product_type = 'INDEFINI', 'INDEFINI', product_exploit) as product_exploit,
    if(product_type = 'INDEFINI', 'INDEFINI', product_family) as product_family,
    if(product_type = 'INDEFINI', 'INDEFINI', product_group) as product_group,
    product_type,
    purchase_unit_price,
    if(product_type = 'INDEFINI', 'INDEFINI', product_classabc) as product_classabc,
    if(product_type = 'INDEFINI', 'INDEFINI', product_planoete) as product_planoete,
    if(product_type = 'INDEFINI', 'INDEFINI', product_planohiver) as product_planohiver,
    if(product_type = 'INDEFINI', 'INDEFINI', product_hpalme) as product_hpalme,
    if(product_type = 'INDEFINI', 'INDEFINI', product_bio) as product_bio,
    is_active
from typed
